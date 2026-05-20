from __future__ import annotations

import argparse
import logging
import subprocess
import sys

import boto3
import setup_athena
import validate_task3
from common import (
    BASE_DIR,
    configure_logging,
    load_environment,
    terraform_apply_env,
)

TERRAFORM_DIR = BASE_DIR / "terraform"
WORKGROUP_RESOURCE = "aws_athena_workgroup.analytics"


def terraform_resource_in_state(terraform_env: dict[str, str], address: str) -> bool:
    result = subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_DIR}", "state", "list"],
        check=True,
        capture_output=True,
        text=True,
        env=terraform_env,
    )
    resources = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    return address in resources


def athena_workgroup_exists(workgroup_name: str) -> bool:
    client = boto3.client("athena", region_name=terraform_apply_env()["TF_VAR_aws_region"])
    try:
        client.get_work_group(WorkGroup=workgroup_name)
        return True
    except client.exceptions.InvalidRequestException:
        return False


def import_existing_resources(terraform_env: dict[str, str]) -> None:
    workgroup_name = terraform_env["TF_VAR_athena_workgroup_name"]
    if not athena_workgroup_exists(workgroup_name):
        return

    if terraform_resource_in_state(terraform_env, WORKGROUP_RESOURCE):
        return

    logging.info(
        "Workgroup %s ja existe na AWS. Importando para o state local do Terraform.",
        workgroup_name,
    )
    subprocess.run(
        [
            "terraform",
            f"-chdir={TERRAFORM_DIR}",
            "import",
            WORKGROUP_RESOURCE,
            workgroup_name,
        ],
        check=True,
        env=terraform_env,
    )


def run_terraform(dry_run: bool, auto_approve: bool) -> int:
    terraform_env = terraform_apply_env()

    logging.info("Passo 1/4 - Inicializando Terraform")
    subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_DIR}", "init"],
        check=True,
        env=terraform_env,
    )

    import_existing_resources(terraform_env)

    if dry_run:
        logging.info("Passo 2/4 - Executando dry-run com terraform plan")
        subprocess.run(
            ["terraform", f"-chdir={TERRAFORM_DIR}", "plan"],
            check=True,
            env=terraform_env,
        )
        logging.info("Dry-run concluido. Encerrando sem aplicar mudancas.")
        return 0

    logging.info("Passo 2/4 - Aplicando infraestrutura base da task 3")
    apply_command = ["terraform", f"-chdir={TERRAFORM_DIR}", "apply"]
    if auto_approve:
        apply_command.append("-auto-approve")
    subprocess.run(apply_command, check=True, env=terraform_env)
    return 0


def run_step(step_name: str, step_number: str, fn) -> int:
    logging.info("%s - %s", step_number, step_name)
    return fn()


def main() -> int:
    parser = argparse.ArgumentParser(description="Orquestrador da task 3")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa apenas terraform init + terraform plan",
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Executa terraform apply com -auto-approve",
    )
    args = parser.parse_args()

    configure_logging()
    load_environment()
    logging.info("=== INICIANDO TASK 3 ===")

    try:
        terraform_status = run_terraform(
            dry_run=args.dry_run, auto_approve=args.auto_approve
        )
        if terraform_status != 0 or args.dry_run:
            return terraform_status

        setup_status = run_step(
            "Registrando tabelas externas dinamicamente",
            "Passo 3/4",
            setup_athena.main,
        )
        if setup_status != 0:
            return setup_status

        validation_status = run_step(
            "Validando consultas analiticas",
            "Passo 4/4",
            validate_task3.main,
        )
        if validation_status != 0:
            return validation_status
    except subprocess.CalledProcessError as exc:
        logging.error(
            "Falha ao executar Terraform. Codigo de saida: %s", exc.returncode
        )
        return 1

    logging.info("=== TASK 3 CONCLUIDA COM SUCESSO ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
