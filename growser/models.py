from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship
from sqlalchemy import Column, DateTime, Float, Integer, SmallInteger, String

from growser.app import app


db = SQLAlchemy(app)


class Repository(db.Model):
    repo_id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)
    owner = Column(String(256), nullable=False)
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
