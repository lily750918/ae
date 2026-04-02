"""使用币安官方 Python SDK 构造 Spot / U 本位合约 REST 等客户端。"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from binance_common.configuration import ConfigurationRestAPI
from binance_common.constants import SPOT_REST_API_TESTNET_URL
from binance_sdk_derivatives_trading_usds_futures import (
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL,
)
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)
from binance_sdk_spot import Spot

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None  # type: ignore[misc, assignment]

_ENV_LOADED = False


def _ensure_dotenv() -> None:
    global _ENV_LOADED
    if _ENV_LOADED or load_dotenv is None:
        return
    root = Path(__file__).resolve().parent
    for candidate in (root / ".env", root.parent / ".env"):
        if candidate.is_file():
            load_dotenv(dotenv_path=candidate)
            break
    _ENV_LOADED = True


def build_rest_config(
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    base_path: Optional[str] = None,
    **kwargs,
) -> ConfigurationRestAPI:
    """与两个 SDK 共用的 REST 配置（api_key / api_secret 可为空，仅供公开接口）。"""
    _ensure_dotenv()
    return ConfigurationRestAPI(
        api_key=api_key if api_key is not None else os.getenv("BINANCE_API_KEY"),
        api_secret=api_secret if api_secret is not None else os.getenv("BINANCE_API_SECRET"),
        base_path=base_path,
        **kwargs,
    )


def spot_client(
    *,
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    base_path: Optional[str] = None,
    testnet: bool = False,
    **config_kwargs,
) -> Spot:
    """
    现货 `Spot` 客户端。`base_path` 为空时：主网默认由 SDK 设为生产 URL；`testnet=True` 时使用现货测试网。
    访问方式：`client.rest_api`、`client.websocket_api`、`client.websocket_streams`。
    """
    _ensure_dotenv()
    if base_path is None and testnet:
        base_path = SPOT_REST_API_TESTNET_URL
    cfg = build_rest_config(
        api_key=api_key,
        api_secret=api_secret,
        base_path=base_path,
        **config_kwargs,
    )
    return Spot(config_rest_api=cfg)


def usds_futures_client(
    *,
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    base_path: Optional[str] = None,
    testnet: bool = False,
    **config_kwargs,
) -> DerivativesTradingUsdsFutures:
    """
    U 本位永续／合约 `DerivativesTradingUsdsFutures` 客户端。
    `base_path` 为空时根据 `testnet` 选择生产或测试网 REST 根地址。
    """
    _ensure_dotenv()
    if base_path is None:
        base_path = (
            DERIVATIVES_TRADING_USDS_FUTURES_REST_API_TESTNET_URL
            if testnet
            else DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
        )
    cfg = build_rest_config(
        api_key=api_key,
        api_secret=api_secret,
        base_path=base_path,
        **config_kwargs,
    )
    return DerivativesTradingUsdsFutures(config_rest_api=cfg)


class BinanceSDKClients:
    """同一时间持有 Spot 与 U 本位合约客户端（各自独立 REST 配置）。"""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        spot_testnet: bool = False,
        futures_testnet: bool = False,
        spot_base_path: Optional[str] = None,
        futures_base_path: Optional[str] = None,
        **config_kwargs,
    ) -> None:
        self.spot = spot_client(
            api_key=api_key,
            api_secret=api_secret,
            base_path=spot_base_path,
            testnet=spot_testnet,
            **config_kwargs,
        )
        self.usds_futures = usds_futures_client(
            api_key=api_key,
            api_secret=api_secret,
            base_path=futures_base_path,
            testnet=futures_testnet,
            **config_kwargs,
        )

    @property
    def client(self) -> DerivativesTradingUsdsFutures:
        """与常见命名一致：默认指 U 本位合约客户端（等同 `usds_futures`）。"""
        return self.usds_futures
