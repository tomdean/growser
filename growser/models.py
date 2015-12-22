import hashlib

from sqlalchemy.orm import relationship
from sqlalchemy import Column, Date, DateTime, Float, Integer, \
    SmallInteger, String
from sqlalchemy.ext.declarative import declared_attr
from flask_sqlalchemy import BaseQuery

from growser.app import db


class Repository(db.Model):
    repo_id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)
    owner = Column(String(256), nullable=False)
    homepage = Column(String(256), nullable=False)
    organization = Column(String(256), nullable=False)
    language = Column(String(32), nullable=False)
    description = Column(String(2048), nullable=False)
    num_events = Column(Integer, nullable=False)
    num_unique = Column(Integer, nullable=False)
    num_stars = Column(Integer, nullable=False)
    num_forks = Column(Integer, nullable=False)
    num_watchers = Column(Integer, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False)

    @property
    def hashid(self):
        return hashlib.md5(self.name.encode('utf-8')).hexdigest()[:12]


class Login(db.Model):
    login_id = Column(Integer, primary_key=True)
    login = Column(String(64), nullable=False)
    num_events = Column(Integer, nullable=False)
    num_unique = Column(Integer, nullable=False)
    created_at = Column(Integer, nullable=False)


class Rating(db.Model):
    login_id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, primary_key=True)
    rating = Column(SmallInteger, nullable=False)
    created_at = Column(DateTime, nullable=False)


class RecommendationModel(db.Model):
    model_id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)


class Recommendation(db.Model):
    model_id = Column(Integer, nullable=False, primary_key=True)
    repo_id = Column(Integer, primary_key=True)
    recommended_repo_id = Column(Integer, primary_key=True)
    score = Column(Float, nullable=False)
    repository = relationship(
        Repository,
        foreign_keys=Repository.repo_id,
        primaryjoin='Recommendation.recommended_repo_id == Repository.repo_id',
        lazy="joined",
        uselist=False
    )

    def __init__(self, model_id: int, repo_id: int,
                 recommended_repo_id: int, score: float):
        self.repo_id = repo_id
        self.recommended_repo_id = recommended_repo_id
        self.model_id = model_id
        self.score = score


class _BaseRanking(object):
    repo_id = Column(Integer, nullable=False, primary_key=True)
    rank = Column(Integer, nullable=False)
    num_events = Column(Integer, nullable=False)

    @declared_attr
    def repository(self):
        return relationship(
            Repository,
            foreign_keys=Repository.repo_id,
            primaryjoin=Repository.repo_id == self.repo_id,
            uselist=False,
            lazy="joined"
        )


class Language(db.Model):
    language_id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String(64), nullable=False)
    created_at = Column(DateTime, nullable=False)
    rank = Column(Integer, nullable=False)

    @staticmethod
    def top():
        return Language.query.filter(Language.rank < 21).order_by(Language.name)


class _DateRanking(_BaseRanking):
    date = Column(Date, nullable=False, primary_key=True)


class _LanguageRanking(object):
    language = Column(String(32), nullable=False, primary_key=True)


class WeeklyRanking(_DateRanking, db.Model):
    pass


class MonthlyRanking(_DateRanking, db.Model):
    pass


class WeeklyRankingByLanguage(_DateRanking, _LanguageRanking, db.Model):
    pass


class MonthlyRankingByLanguage(_DateRanking, _LanguageRanking, db.Model):
    pass


class AllTimeRanking(_BaseRanking, db.Model):
    pass


class AllTimeRankingByLanguage(_BaseRanking, _LanguageRanking, db.Model):
    pass


class Badge(db.Model):
    badge_id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String(32), nullable=False)
    frequency = Column(String(1), nullable=False)


class Badges(db.Model):
    badge_id = Column(Integer, nullable=False, primary_key=True)
    awarded_on = Column(DateTime, nullable=False, primary_key=True)
    repo_id = Column(Integer, nullable=False, primary_key=True)
    votes = Column(Integer, nullable=False)
    total = Column(Integer, nullable=False)
    votes_pct = Column(Float, nullable=False)

    badge = relationship(
        Badge,
        foreign_keys=Badge.badge_id,
        primaryjoin="Badges.badge_id == Badge.badge_id",
        uselist=False,
        lazy="joined"
    )


def _to_dict_model(self):
    return dict((key, getattr(self, key)) for key in self.__mapper__.c.keys())


def _to_dict_query(self):
    return [row.to_dict() for row in self.all()]


db.Model.to_dict = _to_dict_model
BaseQuery.to_dict = _to_dict_query
