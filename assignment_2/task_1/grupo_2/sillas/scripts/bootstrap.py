import logging
import subprocess
import sys
from pathlib import Path

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("Bootstrap_A1")


def run_command(args: list[str], cwd: Path) -> None:
    logger.info(f"Executando: {' '.join(args)} em {cwd}")
    try:
        subprocess.run(args, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar comando: {e}")
        sys.exit(e.returncode)


def main():
    # Mapeamento de caminhos relativos via pathlib
    script_dir = Path(__file__).resolve().parent
    a2_task_dir = script_dir.parent
    repo_root = a2_task_dir.parent.parent.parent.parent
    
    a1_task_dir = repo_root / "assignment_1" / "task_2" / "grupo_2" / "final"
    a1_terraform_dir = a1_task_dir / "terraform"

    logger.info("--- Inicializando Pré-requisitos (Assignment 1) ---")

    # 1. Aplicar Infraestrutura AWS via Terraform (sem necessidade de tfvars)
    logger.info("Provisionando RDS via Terraform")
    run_command(["terraform", "init"], cwd=a1_terraform_dir)
    run_command(["terraform", "apply", "-auto-approve"], cwd=a1_terraform_dir)

    # 2. Sincronizar dependências no Assignment 1
    logger.info("Sincronizando dependências do Assignment 1")
    run_command(["uv", "sync"], cwd=a1_task_dir)

    # 3. Carregar dados do ClassicModels no RDS (usando fallbacks do common.py)
    logger.info("Carregando banco de dados classicmodels original")
    run_command(["uv", "run", "python", "scripts/load_classicmodels.py"], cwd=a1_task_dir)

    logger.info("--- Pré-requisitos configurados com SUCESSO! ---")


if __name__ == "__main__":
    main()
