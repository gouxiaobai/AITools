import argparse

from core.config import Cfg
from core.notion_client import NotionClient
from services.recommendation.runner import RecommendationRunner


def recommend_prices(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    runner = RecommendationRunner(client, cfg)
    req = runner.request_from_args(args)
    return runner.recommend_prices(req)


def backtest_recommendation(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    runner = RecommendationRunner(client, cfg)
    req = runner.request_from_args(args)
    return runner.backtest_recommendation(req)
