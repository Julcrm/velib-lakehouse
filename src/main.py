

"""
Ce module est le point d'entrée de l'application.
Il charge les variables d'environnement, configure le logging et démarre la logique principale.
"""
import os
from dotenv import load_dotenv
from loguru import logger


def main():
    """
    Fonction principale pour exécuter la logique de l'application.

    Étapes:
    1. Charger les variables d'environnement.
    2. Configurer le logging.
    3. Exécuter la logique métier.
    """
    # 1. Charge les variables d'environnement
    load_dotenv()

    # 2. Récupère une variable (exemple)
    env = os.getenv("ENV_NAME", "Local")

    # 3. Log structuré (mieux que print)
    logger.info(f"Starting Velib Lakehouse in {env} mode...")

    # Ton code ici
    print("Hello from the High-Level Template!")


if __name__ == "__main__":
    main()