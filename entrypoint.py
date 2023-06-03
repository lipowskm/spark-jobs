import argparse
import importlib
import inspect
import json
import sys
import time
from typing import Dict, List, Optional

from utils.log4j import Log4j
from utils.spark import get_spark_session


def collect_args(args: Optional[List[str]]) -> Dict[str, str]:
    """
    Convert list of "=" separated arguments into dictionary.

    :param args: list of arguments
    :return: dictionary of arguments
    """
    args_dict = {}
    if args:
        for arg_str in args:
            k, v = arg_str.split("=", 1)
            args_dict[k] = v
    return args_dict


def main() -> None:
    """Main point of execution."""
    parser = argparse.ArgumentParser(description="Run a PySpark job")
    parser.add_argument(
        "--job",
        type=str,
        required=True,
        dest="job_name",
        help="The name of the job module you want to run.",
    )
    parser.add_argument(
        "--job-arg",
        type=str,
        action="append",
        help="Extra arguments to send to the PySpark job. Example: --job-arg foo=bar",
    )
    parser.add_argument(
        "--env-var",
        type=str,
        action="append",
        help="Env vars to be passed to cluster. Example: --env-var foo=bar",
    )
    parser.add_argument(
        "--use-boto3",
        action="store_true",
        help="Configure Spark with boto3 credentials for AWS. "
        "Use during local development for AWS access.",
    )
    args = parser.parse_args()
    job_args = collect_args(args.job_arg)
    env_vars = collect_args(args.env_var)
    spark = get_spark_session(
        args.job_name, env_vars=env_vars, use_boto3=args.use_boto3
    )
    logger = Log4j(spark, log_level="ALL")
    try:
        job_module = importlib.import_module(f"jobs.{args.job_name}")
    except ImportError:
        logger.error(f"No job named {args.job_name}")
        sys.exit(1)
    logger.info(
        f"Running job {args.job_name}..."
        f"\nJob arguments: {json.dumps(job_args, indent=4)}"
        f"\nEnvironmental variables: {json.dumps(env_vars, indent=4)}"
    )
    start = time.time()
    try:
        job_module.perform(spark, **job_args)
    except TypeError:
        logger.error(
            "Invalid or missing job arguments."
            "\nPossible arguments: "
            f"{inspect.getfullargspec(job_module.perform).args[1:]}"
        )
        sys.exit(1)
    except AttributeError:
        logger.error("Job module is missing 'perform' function")
        sys.exit(1)

    end = time.time()
    logger.info(
        f"Execution of job {args.job_name} took {round(end - start, 2)} seconds"
    )


if __name__ == "__main__":
    main()
