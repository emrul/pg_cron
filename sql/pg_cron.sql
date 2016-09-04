CREATE SCHEMA cron;

CREATE SEQUENCE cron.jobid_seq;
CREATE SEQUENCE cron.runid_seq;

CREATE TABLE cron.job (
	jobid bigserial primary key,
	schedule text not null,
	command text not null,
	nodename text not null default 'localhost',
	nodeport int not null default inet_server_port(),
	database text not null default current_database(),
	username text not null default current_user
);

CREATE TABLE cron.result (
	runid bigserial primary key,
	jobid bigint not null,
	starttime timestamptz,
	endtime timestamptz,
	status int not null,
	output text,
	FOREIGN KEY (jobid) REFERENCES cron.job (jobid)
);

/*
CREATE FUNCTION cron.schedule(schedule text, command text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$pg_cron_schedule$$;
COMMENT ON FUNCTION cron.schedule(text,text)
    IS 'schedule a pg_cron job';

CREATE FUNCTION cron.unschedule(job_id bigint)
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$pg_cron_schedule$$;
COMMENT ON FUNCTION cron.unschedule(bigint)
    IS 'unschedule a pg_cron job';
*/
