from collections import namedtuple
import datetime
import hashlib

from sqlalchemy.orm import relationship
from sqlalchemy import (Column, Date, DateTime, Float, Integer,
                        SmallInteger, String, Text, or_)
from growser.app import db


Image = namedtuple('Image', ['path'])
ImageSet = namedtuple('ImageSet', ['small', 'medium', 'large'])


class Repository(db.Model):
    repo_id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)
    owner = Column(String(256), nullable=False)
    homepage = Column(String(256), nullable=False)
    language = Column(String(32), nullable=False)
    description = Column(String(2048), nullable=False)
    num_events = Column(Integer, nullable=False)
    num_stars = Column(Integer, nullable=False)
    num_forks = Column(Integer, nullable=False)
    num_watchers = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    last_release_at = Column(DateTime)
    status = Column(Integer, nullable=False, default=1)

    @property
    def short_name(self):
        return self.name.split('/')[1]

    @property
    def github_url(self):
        return "https://github.com/" + self.name

    @property
    def hashid(self):
        return hashlib.md5(self.name.encode('utf-8')).hexdigest()[:12]

    def __repr__(self):
        return "Repository(repo_id={}, name={}, language={})".format(
                self.repo_id, self.name, self.language)


class Login(db.Model):
    login_id = Column(Integer, primary_key=True)
    login = Column(String(64), nullable=False)
    created_at = Column(Date, nullable=False)


class Release(db.Model):
    release_id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, nullable=False)
    login_id = Column(Integer, nullable=False)
    url = Column(String(256), nullable=False)
    name = Column(String(256), nullable=False)
    tag = Column(String(64), nullable=False)
    body = Column(Text, nullable=False)
    published_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False)

    repository = relationship(
        Repository,
        foreign_keys=Repository.repo_id,
        primaryjoin='Release.repo_id == Repository.repo_id',
        lazy="joined",
        uselist=False
    )

    login = relationship(
        Login,
        foreign_keys=Login.login_id,
        primaryjoin='Release.login_id == Login.login_id',
        lazy="joined",
        uselist=False
    )


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

    @staticmethod
    def find_by_repository(model_id: int, repo_id: int, limit: int = None):
        query = Recommendation.query \
            .filter(Recommendation.model_id == model_id) \
            .filter(Recommendation.repo_id == repo_id) \
            .order_by(Recommendation.score.desc())

        if limit:
            query = query.limit(limit)

        return query.all()


class Language(db.Model):
    language_id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String(64), nullable=False)
    created_at = Column(DateTime, nullable=False)
    rank = Column(Integer, nullable=False)

    @staticmethod
    def top(limit=25):
        predicate = or_(
            Language.rank <= limit,
            Language.name.in_(['Julia', 'Rust', 'Kotlin', 'Erlang', 'Hack']))
        return Language.query.filter(predicate).order_by(Language.name).all()


class Ranking(db.Model):
    language = Column(String(32), nullable=False, primary_key=True)
    period = Column(Integer, nullable=False, primary_key=True)
    repo_id = Column(Integer, primary_key=True)
    start_date = Column(Date)
    end_date = Column(Date)
    rank = Column(Integer, nullable=False)
    num_events = Column(Integer, nullable=False)

    repository = relationship(
        Repository,
        foreign_keys=Repository.repo_id,
        primaryjoin='Ranking.repo_id == Repository.repo_id',
        uselist=False,
        lazy="joined"
    )


class RankingPeriod:
    AllTime = 1
    Monthly = 2
    Weekly = 3
    Recent = 4

    Month = 2
    Week = 3
    Year = 5


class RepositoryTask(db.Model):
    def __init__(self, repo_id, name):
        self.repo_id = repo_id
        self.name = name
        self.created_at = datetime.datetime.now()

    task_id = Column(Integer, nullable=False, primary_key=True)
    repo_id = Column(Integer, nullable=False)
    name = Column(String(32), nullable=False)
    created_at = Column(DateTime, nullable=False)

    @staticmethod
    def add(repo_id, name):
        task = RepositoryTask(repo_id, name)
        db.session.add(task)
        db.session.commit()


class RepositoryRedirect(db.Model):
    name_previous = Column(String(256), nullable=False, primary_key=True)
    name_updated = Column(String(256), nullable=False, primary_key=True)
