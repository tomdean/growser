from flask_sqlalchemy import BaseQuery, SQLAlchemy

BATCH_SIZE = 50000


def configure(app):
    db = SQLAlchemyAutoCommit(app)
    db.Model.to_dict = _to_dict_model
    BaseQuery.to_dict = _to_dict_query
    return db


def _to_dict_model(self):
    return dict((key, getattr(self, key)) for key in self.__mapper__.c.keys())


def _to_dict_query(self):
    return [row.to_dict() for row in self.all()]


class SQLAlchemyAutoCommit(SQLAlchemy):
    """By default ``psycopg2`` will wrap SELECT statements in a transaction.

    This can be avoided using AUTOCOMMIT to rely on Postgres' default
    implicit transaction mode (see this `blog post <http://bit.ly/1N0a7Lj>`_
    for more details).
    """
    def apply_driver_hacks(self, app, info, options):
        super().apply_driver_hacks(app, info, options)
        options['isolation_level'] = 'AUTOCOMMIT'
