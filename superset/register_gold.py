"""Enregistre (de maniere idempotente) la base GOLD dans Superset.

Lance au demarrage par le conteneur superset-init, une fois les migrations
appliquees. Rejoue sans effet de bord : si la connexion "GOLD" existe deja,
son URI est simplement remis a jour.
"""

import os

from superset.app import create_app

DB_NAME = os.environ.get("GOLD_DB_NAME", "GOLD")
DB_URI = os.environ.get(
    "GOLD_SQLALCHEMY_URI",
    "postgresql+psycopg2://app:app12345@postgres:5432/gold",
)

app = create_app()
with app.app_context():
    from superset import db
    from superset.models.core import Database

    database = (
        db.session.query(Database).filter_by(database_name=DB_NAME).first()
    )
    if database is None:
        database = Database(database_name=DB_NAME)
        db.session.add(database)

    # set_sqlalchemy_uri extrait le mot de passe de l'URI et le chiffre a part.
    database.set_sqlalchemy_uri(DB_URI)
    db.session.commit()
    print(f"[register_gold] base '{DB_NAME}' enregistree -> {database.sqlalchemy_uri}")
