/*-------------------------------------------------------------------------
 *
 * src/pg_cron.c
 *
 * Implementation of the pg_cron task scheduler.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

/* these are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */

#define MAIN_PROGRAM
#include "cron.h"

#include "pg_cron.h"
#include "cron_job.h"

#include "poll.h"
#include "sys/time.h"
#include "sys/poll.h"
#include "time.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "tcop/utility.h"


PG_MODULE_MAGIC;


void _PG_init(void);
void _PG_fini(void);
static void pg_cron_sigterm(SIGNAL_ARGS);
static void pg_cron_sighup(SIGNAL_ARGS);
static void PgCronWorkerMain(Datum arg);

static void StartAllPendingRuns(List *taskList, TimestampTz currentTime);
static void StartPendingRuns(CronTask *task, ClockProgress clockProgress,
							 TimestampTz lastMinute, TimestampTz currentTime);
static int MinutesPassed(TimestampTz startTime, TimestampTz stopTime);
static TimestampTz TimestampMinuteStart(TimestampTz time);
static TimestampTz TimestampMinuteEnd(TimestampTz time);
static bool ShouldRunTask(entry *schedule, TimestampTz currentMinute,
						  bool doWild, bool doNonWild);

static List * CurrentTaskList(void);
static void ReloadCronJobs(void);
static List * LoadCronJobList(void);
static CronJob * TupleToCronJob(TupleDesc tupleDescriptor, HeapTuple heapTuple);

static void WaitForCronTasks(List *taskList);
static void PollForTasks(List *taskList);
static void ManageCronTasks(List *taskList, TimestampTz currentTime);
static void ManageCronTask(CronTask *task, TimestampTz currentTime);

static HTAB * CreateCronJobHash(void);
static HTAB * CreateCronTaskHash(void);
static CronJob * GetCronJob(int64 jobId);
static CronTask * GetCronTask(int64 jobId);
static void InitializeCronTask(CronTask *task, int64 jobId);


/* flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;

static MemoryContext CronJobContext = NULL;
static MemoryContext CronTaskContext = NULL;
static HTAB *CronJobHash = NULL;
static HTAB *CronTaskHash = NULL;
static bool CronJobCacheValid = false;
static int64 RunCount = 0;

static char *CronTableDatabaseName = "postgres";
static int CronTaskStartTimeout = 10000; /* maximum connection time */
static const int MaxWait = 1000; /* maximum time in ms that poll() can block */


/*
 * _PG_init gets called when the extension is loaded.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("pg_cron can only be loaded via shared_preload_libraries"),
						errhint("Add pg_cron to shared_preload_libraries configuration "
								"variable in postgresql.conf in master and workers.")));
	}

	DefineCustomStringVariable(
		"cron.database_name",
		gettext_noop("Database in which pg_cron metadata is kept."),
		NULL,
		&CronTableDatabaseName,
		"postgres",
		PGC_POSTMASTER,
		GUC_SUPERUSER_ONLY,
		NULL, NULL, NULL);

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = PgCronWorkerMain;
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;
	sprintf(worker.bgw_library_name, "pg_cron");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_cron_scheduler");

	RegisterBackgroundWorker(&worker);
}


/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_cron_sigterm(SIGNAL_ARGS)
{
	got_sigterm = true;
}


/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reload the cron jobs.
 */
static void
pg_cron_sighup(SIGNAL_ARGS)
{
	CronJobCacheValid = false;
}


/*
 * PgCronWorkerMain is the main entry-point for the background worker
 * that performs tasks.
 */
static void
PgCronWorkerMain(Datum arg)
{
	MemoryContext CronLoopContext = NULL;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_cron_sighup);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, pg_cron_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(CronTableDatabaseName, NULL);

	CronJobContext = AllocSetContextCreate(CurrentMemoryContext,
										   "pg_cron job context",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

	CronTaskContext = AllocSetContextCreate(CurrentMemoryContext,
											"pg_cron task context",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);

	CronLoopContext = AllocSetContextCreate(CurrentMemoryContext,
											"pg_cron loop context",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);

	CronJobHash = CreateCronJobHash();
	CronTaskHash = CreateCronTaskHash();

	elog(LOG, "pg_cron scheduler started");

	MemoryContextSwitchTo(CronLoopContext);

	while (!got_sigterm)
	{
		List *taskList = NIL;
		TimestampTz currentTime = 0;

		if (!CronJobCacheValid)
		{
			ReloadCronJobs();
			CronJobCacheValid = true;
			elog(LOG, "reloaded cron jobs");
		}

		taskList = CurrentTaskList();
		currentTime = GetCurrentTimestamp();

		StartAllPendingRuns(taskList, currentTime);

		WaitForCronTasks(taskList);
		ManageCronTasks(taskList, currentTime);

		MemoryContextReset(CronLoopContext);
	}

	elog(LOG, "pg_cron scheduler shutting down");

	proc_exit(0);
}


/*
 * ReloadCronJobs reloads the cron jobs from the cron.job table.
 * If a job that has an active task has been removed, the task
 * is marked as inactive by this function.
 */
static void
ReloadCronJobs(void)
{
	List *jobList = NIL;
	ListCell *jobCell = NULL;
	CronTask *task = NULL;
	HASH_SEQ_STATUS status;

	/* destroy old job hash */
	MemoryContextResetAndDeleteChildren(CronJobContext);

	CronJobHash = CreateCronJobHash();

	hash_seq_init(&status, CronTaskHash);

	/* mark all tasks as inactive */
	while ((task = hash_seq_search(&status)) != NULL)
	{
		task->isActive = false;
	}

	jobList = LoadCronJobList();

	/* mark tasks that still have a job as active */
	foreach(jobCell, jobList)
	{
		CronJob *job = (CronJob *) lfirst(jobCell);

		CronTask *task = GetCronTask(job->jobId);
		task->isActive = true;
	}
}


/*
 * LoadCronJobList loads the current list of jobs from the
 * cron.job table.
 */
static List *
LoadCronJobList(void)
{
	List *jobList = NIL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;
	Oid cronSchemaId = InvalidOid;
	Oid jobRelationId = InvalidOid;
	Relation cronJobTable = NULL;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	cronSchemaId = get_namespace_oid("cron", false);
	jobRelationId = get_relname_relid("job", cronSchemaId);

	cronJobTable = heap_open(jobRelationId, AccessShareLock);

	scanDescriptor = systable_beginscan(cronJobTable,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(cronJobTable);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		MemoryContext oldContext = NULL;
		CronJob *job = NULL;

		oldContext = MemoryContextSwitchTo(CronJobContext);

		job = TupleToCronJob(tupleDescriptor, heapTuple);
		jobList = lappend(jobList, job);

		MemoryContextSwitchTo(oldContext);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(cronJobTable, AccessShareLock);

	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	return jobList;
}


/*
 * TupleToCronJob takes a heap tuple and converts it into a CronJob
 * struct.
 */
static CronJob *
TupleToCronJob(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	CronJob *job = NULL;
	int64 jobKey = 0;
	bool isNull = false;
	bool isPresent = false;
	entry *parsedSchedule = NULL;

	Datum jobId = heap_getattr(heapTuple, Anum_cron_job_jobid,
							   tupleDescriptor, &isNull);
	Datum schedule = heap_getattr(heapTuple, Anum_cron_job_schedule,
								  tupleDescriptor, &isNull);
	Datum command = heap_getattr(heapTuple, Anum_cron_job_command,
								 tupleDescriptor, &isNull);
	Datum nodeName = heap_getattr(heapTuple, Anum_cron_job_nodename,
								  tupleDescriptor, &isNull);
	Datum nodePort = heap_getattr(heapTuple, Anum_cron_job_nodeport,
								  tupleDescriptor, &isNull);
	Datum database = heap_getattr(heapTuple, Anum_cron_job_database,
								  tupleDescriptor, &isNull);
	Datum userName = heap_getattr(heapTuple, Anum_cron_job_username,
								  tupleDescriptor, &isNull);

	Assert(!HeapTupleHasNulls(heapTuple));

	jobKey = DatumGetUInt32(jobId);
	job = hash_search(CronJobHash, &jobKey, HASH_ENTER, &isPresent);

	job->jobId = DatumGetUInt32(jobId);
	job->scheduleText = TextDatumGetCString(schedule);
	job->command = TextDatumGetCString(command);
	job->nodeName = TextDatumGetCString(nodeName);
	job->nodePort = DatumGetUInt32(nodePort);
	job->userName = TextDatumGetCString(userName);
	job->database = TextDatumGetCString(database);

	parsedSchedule = parse_cron_entry(job->scheduleText);
	if (parsedSchedule != NULL)
	{
		/* copy the schedule and free the allocated memory immediately */

		job->schedule = *parsedSchedule;
		free_entry(parsedSchedule);
	}
	else
	{
		elog(LOG, "invalid pg_cron schedule for job %ld: %s", jobId, job->scheduleText);
	}

	return job;
}

/*
 * StartPendingRuns goes through the list of tasks and kicks of
 * runs for tasks that should start, taking clock changes into
 * into consideration.
 */
static void
StartAllPendingRuns(List *taskList, TimestampTz currentTime)
{
	static TimestampTz lastMinute = 0;

	int minutesPassed = 0;
	ListCell *taskCell = NULL;
	ClockProgress clockProgress;

	if (lastMinute == 0)
	{
		lastMinute = TimestampMinuteStart(currentTime);
	}

	minutesPassed = MinutesPassed(lastMinute, currentTime);
	if (minutesPassed == 0)
	{
		/* wait for new minute */
		return;
	}

	/* use Vixie cron logic for clock jumps */
	if (minutesPassed > (3*MINUTE_COUNT))
	{
		/* clock jumped forward by more than 3 hours */
		clockProgress = CLOCK_CHANGE;
	}
	else if (minutesPassed > 5)
	{
		/* clock went forward by more than 5 minutes (DST?) */
		clockProgress = CLOCK_JUMP_FORWARD;
	}
	else if (minutesPassed > 0)
	{
		/* clock went forward by 1-5 minutes */
		clockProgress = CLOCK_PROGRESSED;
	}
	else if (minutesPassed > -(3*MINUTE_COUNT))
	{
		/* clock jumped backwards by less than 3 hours (DST?) */
		clockProgress = CLOCK_JUMP_BACKWARD;
	}
	else
	{
		/* clock jumped backwards 3 hours or more */
		clockProgress = CLOCK_CHANGE;
	}

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);

		StartPendingRuns(task, clockProgress, lastMinute, currentTime);
	}

	/*
	 * If the clock jump backwards then we avoid repeating the fixed-time
	 * tasks by preserving the last minute from before the clock jump,
	 * until the clock has caught up (clockProgress will be
	 * CLOCK_JUMP_BACKWARD until then).
	 */
	if (clockProgress != CLOCK_JUMP_BACKWARD)
	{
		lastMinute = TimestampMinuteStart(currentTime);
	}
}



/*
 * StartPendingRuns kicks off pending runs for a task if it
 * should start, taking clock changes into consideration.
 */
static void
StartPendingRuns(CronTask *task, ClockProgress clockProgress,
				 TimestampTz lastMinute, TimestampTz currentTime)
{
	CronJob *cronJob = GetCronJob(task->jobId);
	entry *schedule = &cronJob->schedule;
	TimestampTz virtualTime = lastMinute;
	TimestampTz currentMinute = TimestampMinuteStart(currentTime);


	switch (clockProgress)
	{
		case CLOCK_PROGRESSED:
		{
			/*
			 * case 1: minutesPassed is a small positive number
			 * run jobs for each virtual minute until caught up.
			 */

			do
			{
				virtualTime = TimestampTzPlusMilliseconds(virtualTime,
														  60*1000);

				if (ShouldRunTask(schedule, virtualTime, true, true))
				{
					task->pendingRunCount += 1;
				}
			}
			while (virtualTime < currentMinute);

			break;
		}

		case CLOCK_JUMP_FORWARD:
		{
			/*
			 * case 2: minutesPassed is a medium-sized positive number,
			 * for example because we went to DST run wildcard
			 * jobs once, then run any fixed-time jobs that would
			 * otherwise be skipped if we use up our minute
			 * (possible, if there are a lot of jobs to run) go
			 * around the loop again so that wildcard jobs have
			 * a chance to run, and we do our housekeeping
			 */

			/* run fixed-time jobs for each minute missed */ 
			do
			{
				virtualTime = TimestampTzPlusMilliseconds(virtualTime,
														  60*1000);

				if (ShouldRunTask(schedule, virtualTime, false, true))
				{
					task->pendingRunCount += 1;
				}

			} while (virtualTime < currentMinute);

			/* run wildcard jobs for current minute */
			if (ShouldRunTask(schedule, currentMinute, true, false))
			{
				task->pendingRunCount += 1;
			}	

			break;
		}

		case CLOCK_JUMP_BACKWARD:
		{
			/*
			 * case 3: timeDiff is a small or medium-sized
			 * negative num, eg. because of DST ending just run
			 * the wildcard jobs. The fixed-time jobs probably
			 * have already run, and should not be repeated
			 * virtual time does not change until we are caught up
			 */

			if (ShouldRunTask(schedule, currentMinute, true, false))
			{
				task->pendingRunCount += 1;
			}	

			break;
		}

		default:
		{
			/*
			 * other: time has changed a *lot*, skip over any
			 * intermediate fixed-time jobs and go back to
			 * normal operation.
			 */
			if (ShouldRunTask(schedule, currentMinute, true, true))
			{
				task->pendingRunCount += 1;
			}
		}
	}
}


/*
 * MinutesPassed returns the number of minutes between startTime and
 * stopTime rounded down to the closest integer.
 */
static int
MinutesPassed(TimestampTz startTime, TimestampTz stopTime)
{
	int microsPassed = 0;
	long secondsPassed = 0;
	int minutesPassed = 0;

	TimestampDifference(startTime, stopTime,
						&secondsPassed, &microsPassed);

	minutesPassed = secondsPassed / 60;

	return minutesPassed;
}


/*
 * TimestampMinuteEnd returns the timestamp at the start of the
 * current minute for the given time.
 */
static TimestampTz
TimestampMinuteStart(TimestampTz time)
{
	TimestampTz result = 0;

#ifdef HAVE_INT64_TIMESTAMP
	result = time - time % 60000000;
#else
	result = (long) time - (long) time % 60;
#endif

	return result;
}


/*
 * TimestampMinuteEnd returns the timestamp at the start of the
 * next minute from the given time.
 */
static TimestampTz
TimestampMinuteEnd(TimestampTz time)
{
	TimestampTz result = TimestampMinuteStart(time);

#ifdef HAVE_INT64_TIMESTAMP
	result += 60000000;
#else
	result += 60;
#endif

	return result;
}

	
/*
 * ShouldRunTask returns whether a job should run in the current
 * minute according to its schedule.
 */
static bool
ShouldRunTask(entry *schedule, TimestampTz currentTime, bool doWild,
			  bool doNonWild)
{
	time_t currentTime_t = timestamptz_to_time_t(currentTime);
	struct tm *tm = gmtime(&currentTime_t);

	int minute = tm->tm_min -FIRST_MINUTE;
	int hour = tm->tm_hour -FIRST_HOUR;
	int dayOfMonth = tm->tm_mday -FIRST_DOM;
	int month = tm->tm_mon +1 -FIRST_MONTH;
	int dayOfWeek = tm->tm_wday -FIRST_DOW;

	if (bit_test(schedule->minute, minute) &&
	    bit_test(schedule->hour, hour) &&
	    bit_test(schedule->month, month) &&
	    ( ((schedule->flags & DOM_STAR) || (schedule->flags & DOW_STAR))
	      ? (bit_test(schedule->dow,dayOfWeek) && bit_test(schedule->dom,dayOfMonth))
	      : (bit_test(schedule->dow,dayOfWeek) || bit_test(schedule->dom,dayOfMonth)))) {
		if ((doNonWild && !(schedule->flags & (MIN_STAR|HR_STAR)))
		    || (doWild && (schedule->flags & (MIN_STAR|HR_STAR))))
		{
			return true;
		}
	}

	return false;
}


/*
 * CurrentTaskList extracts the current list of tasks from the
 * cron task hash.
 */
static List *
CurrentTaskList(void)
{
	List *taskList = NIL;
	CronTask *task = NULL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, CronTaskHash);

	while ((task = hash_seq_search(&status)) != NULL)
	{
		taskList = lappend(taskList, task);
	}

	return taskList;
}



/*
 * WaitForCronTasks blocks waiting for any active task for at most
 * 1 second.
 */
static void
WaitForCronTasks(List *taskList)
{
	int taskCount = list_length(taskList);

	if (taskCount > 0)
	{
		PollForTasks(taskList);
	}
	else
	{
		/* wait for new jobs */
		pg_usleep(MaxWait*1000L);
	}
}


/*
 * PollForTasks calls poll() for the sockets of all tasks. It checks for
 * read or write events based on the pollingStatus of the task.
 */
static void
PollForTasks(List *taskList)
{
	TimestampTz currentTime = 0;
	TimestampTz nextEventTime = 0;
	int pollTimeout = 0;
	long waitSeconds = 0;
	int waitMicros = 0;
	struct pollfd *pollFDs = NULL;
	int pollResult = 0;

	int taskIndex = 0;
	int taskCount = list_length(taskList);
	ListCell *taskCell = NULL;

	pollFDs = (struct pollfd *) palloc0(taskCount * sizeof(struct pollfd));

	currentTime = GetCurrentTimestamp();

	/*
	 * At the latest, wake up when the next minute starts.
	 */
	nextEventTime = TimestampMinuteEnd(currentTime);

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);
		PostgresPollingStatusType pollingStatus = task->pollingStatus;
		struct pollfd *pollFileDescriptor = &pollFDs[taskIndex];

		if (task->state == CRON_TASK_WAITING &&
			task->pendingRunCount > 0)
		{
			/*
			 * We have a task that can start, don't wait.
			 */
			pfree(pollFDs);
			return;
		}

		if (task->state == CRON_TASK_CONNECTING ||
			task->state == CRON_TASK_SENDING)
		{
			/*
			 * We need to wake up when a timeout expires.
			 * Take the minimum of nextEventTime and task->startDeadline.
			 */
			if (TimestampDifferenceExceeds(task->startDeadline, nextEventTime, 0))
			{
				nextEventTime = task->startDeadline;
			}
		}

		if (task->state == CRON_TASK_CONNECTING ||
			task->state == CRON_TASK_SENDING ||
			task->state == CRON_TASK_RUNNING)
		{
			PGconn *connection = task->connection;
			int pollEventMask = 0;

			/*
			 * Set the appropriate mask for poll, based on the current polling
			 * status of the task, controlled by ManageCronTask.
			 */

			if (pollingStatus == PGRES_POLLING_READING)
			{
				pollEventMask = POLLERR | POLLIN;
			}
			else if (pollingStatus == PGRES_POLLING_WRITING)
			{
				pollEventMask = POLLERR | POLLOUT;
			}

			pollFileDescriptor->fd = PQsocket(connection);
			pollFileDescriptor->events = pollEventMask;
		}
		else
		{
			/*
			 * Task is not running.
			 */

			pollFileDescriptor->fd = -1;
			pollFileDescriptor->events = 0;
		}

		pollFileDescriptor->revents = 0;

		taskIndex++;
	}

	/*
	 * Find the first time-based event, which is either the start of a new
	 * minute or a timeout.
	 */
	TimestampDifference(currentTime, nextEventTime, &waitSeconds, &waitMicros);

	pollTimeout = waitSeconds * 1000 + waitMicros / 1000;
	if (pollTimeout > MaxWait)
	{
		/*
		 * We never wait more than 1 second, this gives us a chance to react
		 * to external events like a TERM signal and job changes.
		 */

		pollTimeout = MaxWait;
	}

	pollResult = poll(pollFDs, taskCount, pollTimeout);
	if (pollResult < 0)
	{
		/*
		 * This typically happens in case of a signal, though we should
		 * probably check errno in case something bad happened.
		 */

		pfree(pollFDs);
		return;
	}

	taskIndex = 0;

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);
		struct pollfd *pollFileDescriptor = &pollFDs[taskIndex];

		task->isSocketReady = pollFileDescriptor->revents &
							  pollFileDescriptor->events;

		taskIndex++;
	}

	pfree(pollFDs);
}


/*
 * ManageCronTasks proceeds the state machines of the given list of tasks.
 */
static void
ManageCronTasks(List *taskList, TimestampTz currentTime)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		CronTask *task = (CronTask *) lfirst(taskCell);

		ManageCronTask(task, currentTime);
	}
}


/*
 * ManageCronTask implements the cron task state machine.
 */
static void
ManageCronTask(CronTask *task, TimestampTz currentTime)
{
	CronTaskState checkState = task->state;
	int64 jobId = task->jobId;
	CronJob *cronJob = GetCronJob(jobId);
	PGconn *connection = task->connection;
	ConnStatusType connectionStatus = CONNECTION_BAD;

	switch (checkState)
	{
		case CRON_TASK_WAITING:
		{
			/* check if job has been removed */
			if (!task->isActive)
			{
				task->errorMessage = "job cancelled";
				task->state = CRON_TASK_ERROR;
				task->pollingStatus = 0;
				break;
			}

			/* check whether runs are pending */
			if (task->pendingRunCount == 0)
			{
				break;
			}

			task->runId = RunCount++;
			task->pendingRunCount -= 1;
			task->state = CRON_TASK_START;
		}

		case CRON_TASK_START:
		{
			const char *clientEncoding = GetDatabaseEncodingName();
			char nodePortString[12];
			TimestampTz startDeadline = 0;

			const char *keywordArray[] = {
				"host",
				"port",
				"fallback_application_name",
				"client_encoding",
				"dbname",
				"user",
				NULL
			};
			const char *valueArray[] = {
				cronJob->nodeName,
				nodePortString,
				"pg_cron",
				clientEncoding,
				cronJob->database,
				cronJob->userName,
				NULL
			};
			sprintf(nodePortString, "%d", cronJob->nodePort);

			Assert(sizeof(keywordArray) == sizeof(valueArray));

			connection = PQconnectStartParams(keywordArray, valueArray, false);
			PQsetnonblocking(connection, 1);

			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				task->state = CRON_TASK_ERROR;
				task->pollingStatus = 0;
				break;
			}

			startDeadline = TimestampTzPlusMilliseconds(currentTime,
														CronTaskStartTimeout);

			task->startDeadline = startDeadline;
			task->connection = connection;
			task->pollingStatus = PGRES_POLLING_WRITING;
			task->state = CRON_TASK_CONNECTING;

			break;
		}

		case CRON_TASK_CONNECTING:
		{
			PostgresPollingStatusType pollingStatus = 0;

			/* check if job has been removed */
			if (!task->isActive)
			{
				task->errorMessage = "job cancelled";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if timeout has been reached */
			if (TimestampDifferenceExceeds(task->startDeadline, currentTime, 0))
			{
				task->errorMessage = "connection timeout";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if connection is still alive */
			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				task->errorMessage = "failed to connect";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check whether a connection has been established */
			pollingStatus = PQconnectPoll(connection);
			if (pollingStatus == PGRES_POLLING_OK)
			{
				/* wait for socket to be ready to send a query */
				task->pollingStatus = PGRES_POLLING_WRITING;

				task->state = CRON_TASK_SENDING;
			}
			else if (pollingStatus == PGRES_POLLING_FAILED)
			{
				task->errorMessage = PQerrorMessage(connection);
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
			}
			else
			{
				/*
				 * Connection is still being established.
				 *
				 * On the next WaitForTasks round, we wait for reading or writing
				 * based on the status returned by PQconnectPoll, see:
				 * https://www.postgresql.org/docs/9.5/static/libpq-connect.html
				 */
				task->pollingStatus = pollingStatus;
			}

			break;
		}

		case CRON_TASK_SENDING:
		{
			char *command = cronJob->command;
			int sendResult = 0;

			/* check if job has been removed */
			if (!task->isActive)
			{
				task->errorMessage = "job cancelled";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if timeout has been reached */
			if (TimestampDifferenceExceeds(task->startDeadline, currentTime, 0))
			{
				task->errorMessage = "connection timeout";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if socket is ready to send */
			if (!task->isSocketReady)
			{
				break;
			}

			/* check if connection is still alive */
			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				task->errorMessage = PQerrorMessage(connection);
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			sendResult = PQsendQuery(connection, command);
			if (sendResult == 1)
			{
				/* wait for socket to be ready to receive results */
				task->pollingStatus = PGRES_POLLING_READING;

				/* command is underway, stop using timeout */
				task->startDeadline = 0;
				task->state = CRON_TASK_RUNNING;
			}
			else
			{
				/* not yet ready to send */
			}	

			break;
		}

		case CRON_TASK_RUNNING:
		{
			int connectionBusy = 0;
			PGresult *result = NULL;

			/* check if job has been removed */
			if (!task->isActive)
			{
				task->errorMessage = "job cancelled";
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if connection is still alive */
			connectionStatus = PQstatus(connection);
			if (connectionStatus == CONNECTION_BAD)
			{
				task->errorMessage = PQerrorMessage(connection);
				task->pollingStatus = 0;
				task->state = CRON_TASK_ERROR;
				break;
			}

			/* check if socket is ready to send */
			if (!task->isSocketReady)
			{
				break;
			}

			PQconsumeInput(connection);

			connectionBusy = PQisBusy(connection);
			if (connectionBusy)
			{
				/* still waiting for results */
				break;
			}

			while ((result = PQgetResult(connection)) != NULL)
			{
				ExecStatusType executionStatus = PQresultStatus(result);

				switch (executionStatus)
				{
					case PGRES_TUPLES_OK:
					{
						break;
					}

					case PGRES_COMMAND_OK:
					{
						break;
					}

					case PGRES_BAD_RESPONSE:
					case PGRES_FATAL_ERROR:
					{
						task->errorMessage = PQresultErrorMessage(result);
						task->pollingStatus = 0;
						task->state = CRON_TASK_ERROR;
						return;
					}

					case PGRES_COPY_IN:
					case PGRES_COPY_OUT:
					case PGRES_COPY_BOTH:
					{
						/* cannot handle COPY input/output */
						task->errorMessage = "COPY not supported";
						task->pollingStatus = 0;
						task->state = CRON_TASK_ERROR;
						return;
					}

					case PGRES_EMPTY_QUERY:
					case PGRES_SINGLE_TUPLE:
					case PGRES_NONFATAL_ERROR:
					default:
					{
						break;
					}

				}

				PQclear(result);
			}

			PQfinish(connection);

			task->connection = NULL;
			task->pollingStatus = 0;
			task->state = CRON_TASK_DONE;

			break;
		}

		case CRON_TASK_ERROR:
		{
			if (task->connection != NULL)
			{
				PQfinish(connection);
				task->connection = NULL;
			}

			if (!task->isActive)
			{
				bool isPresent = false;
				hash_search(CronTaskHash, &jobId, HASH_REMOVE, &isPresent);
			}

			if (task->errorMessage != NULL)
			{
				elog(LOG, "error running job %ld: %s", jobId, task->errorMessage);
			}

			task->startDeadline = 0;
			task->isSocketReady = false;
			task->state = CRON_TASK_DONE;

			/* fall through to CRON_TASK_DONE */
		}

		case CRON_TASK_DONE:
		default:
		{
			/* nothing to do, task is done */

			Assert(task->pollingStatus == 0);
			Assert(task->connection == NULL);
			Assert(task->startDeadline == 0);
			Assert(!task->isSocketReady);

			task->state = CRON_TASK_WAITING;
		}

	}
}


static HTAB *
CreateCronJobHash(void)
{
	HTAB *taskHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(CronJob);
	info.hash = tag_hash;
	info.hcxt = CronJobContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	taskHash = hash_create("pg_cron jobs", 32, &info, hashFlags);

	return taskHash;
}


static HTAB *
CreateCronTaskHash(void)
{
	HTAB *taskHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(CronTask);
	info.hash = tag_hash;
	info.hcxt = CronTaskContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	taskHash = hash_create("pg_cron tasks", 32, &info, hashFlags);

	return taskHash;
}


/*
 * GetCronTask gets the current task with the given job ID.
 */
static CronTask *
GetCronTask(int64 jobId)
{
	CronTask *task = NULL;
	int64 hashKey = jobId;
	bool isPresent = false;

	task = hash_search(CronTaskHash, &hashKey, HASH_ENTER, &isPresent);
	if (!isPresent)
	{
		InitializeCronTask(task, jobId);
	}

	return task;
}


/*
 * InitializeCronTask intializes a CronTask struct.
 */
static void
InitializeCronTask(CronTask *task, int64 jobId)
{
	task->runId = 0;
	task->jobId = jobId;
	task->state = CRON_TASK_WAITING;
	task->pendingRunCount = 0;
	task->connection = NULL;
	task->pollingStatus = 0;
	task->startDeadline = 0;
	task->isSocketReady = false;
	task->isActive = true;
	task->errorMessage = NULL;
}


/*
 * GetCronJob gets the cron job with the given id.
 */
static CronJob *
GetCronJob(int64 jobId)
{
	CronJob *job = NULL;
	int64 hashKey = jobId;
	bool isPresent = false;

	job = hash_search(CronJobHash, &hashKey, HASH_FIND, &isPresent);

	return job;
}
