"""Default job runner script"""

import argparse
import importlib
import logging
import os
import sys
import time

# pylint: disable=ungrouped-imports
try:
    import pyspark
except ImportError:
    import findspark

    findspark.init()
    import pyspark


def config_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%x%X",
    )


def config_syspath():
    """Configure packages path"""
    jobs_path = "jobs.zip" if os.path.exists("jobs.zip") else "./jobs"
    sys.path.insert(0, jobs_path)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job-name", type=str, required=True, dest="job_name", help="Job name"
    )
    parser.add_argument(
        "--job-args",
        nargs="*",
        default=[],
        required=False,
        dest="job_args",
        help="Job extra arguments. E.g --job-args arg1=val1 arg2=val2",
    )
    return parser.parse_args()


def parse_job_args(job_args):
    """Parse job arguments"""
    job_args_dict = {}
    for arg in job_args:
        if "=" in arg:
            key, value = arg.split("=", 1)
            job_args_dict[key] = value
        else:
            logging.warning("Ignoring malformed job argument: %s", arg)
    return job_args_dict


def run_job(job_name, job_args):
    """Run the specified PySpark job."""
    environment = {"PYSPARK_JOB_ARGS": " ".join(job_args) if job_args else ""}
    os.environ.update(environment)

    logging.info("Running job: %s with environment: %s", job_name, environment)

    spark = pyspark.sql.SparkSession.builder.appName(job_name).getOrCreate()
    # appName=job_name, environment=environment)

    try:
        job_module = importlib.import_module(f"jobs.{job_name}")
        if not hasattr(job_module, "run"):
            logging.error(
                "Job module 'jobs.%s' does not have a 'run' function.", job_name
            )
            sys.exit(1)

        start_time = time.time()
        job_module.run(spark, **job_args)
        execution_time = time.time() - start_time

        logging.info(
            "Execution of job '%s' took %.2f seconds", job_name, execution_time
        )
    except ModuleNotFoundError:
        logging.error("Job module 'jobs.%s' not found.", job_name)
        sys.exit(1)
    except Exception as ex:
        logging.exception("Error running job '%s': %s", job_name, ex)
        sys.exit(1)
    finally:
        spark.stop()


def main():
    """Main function"""
    config_logging()
    config_syspath()

    args = parse_args()
    job_name = args.job_name
    job_args = parse_job_args(args.job_args)

    run_job(job_name, job_args)


if __name__ == "__main__":
    main()
