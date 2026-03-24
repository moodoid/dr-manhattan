import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import websockets
import websockets.exceptions

from ...base.websocket import OrderBookWebSocket
from ...models.orderbook import OrderbookManager

logger = logging.getLogger(__name__)


class TradeEvent(Enum):
    """Trade event types from user WebSocket"""

    FILL = "fill"
    PARTIAL_FILL = "partial_fill"


@dataclass
class MakerOrder:
    order_id: str = ""
    owner: str = ""
    maker_address: str = ""
    matched_amount: float = 0.0
    price: float = 0.0
    fee_rate_bps: int = 0
    asset_id: str = ""
    outcome: str = ""
    side: str = ""

@dataclass
class Trade:
    """Represents a trade event."""

    # Core fields used across the codebase.
    id: str
    market_id: str
    asset_id: str
    side: str
    size: float
    price: float
    fee_rate_bps: int
    timestamp: Any

    # Canonical websocket event fields.
    event_type: str = ""
    type: str = "TRADE"
    taker_order_id: str = ""
    status: str = ""
    match_time: str = ""
    last_update: Any = ""
    outcome: str = ""
    owner: str = ""
    trade_owner: str = ""
    maker_address: str = ""
    transaction_hash: str = ""
    bucket_index: int = 0
    maker_orders: List[MakerOrder] = field(default_factory=list)
    trader_side: str = ""

    # Backward-compatible aliases used by legacy callsites/tests.
    order_id: str = ""
    taker: str = ""
    maker: str = ""
    raw_event: Dict[str, Any] | None = None


@dataclass
class OrderEvent:
    """Represents an order event from the user WebSocket."""

    id: str
    market_id: str
    asset_id: str
    side: str
    price: float
    original_size: float
    size_matched: float
    status: str

    event_type: str = "order"
    type: str = ""
    owner: str = ""
    order_owner: str = ""
    outcome: str = ""
    created_at: Any = ""
    expiration: Any = ""
    order_type: str = ""
    maker_address: str = ""
    timestamp: Any = ""
    associate_trades: Any = None

    order_id: str = ""
    market: str = ""
    raw_event: Dict[str, Any] | None = None

    @property
    def remaining_size(self) -> float:
        return max(self.original_size - self.size_matched, 0.0)


class PolymarketWebSocket(OrderBookWebSocket):
    """
    Polymarket WebSocket implementation for real-time orderbook updates.

    Uses CLOB WebSocket API for market channel subscriptions.
    Documentation: https://docs.polymarket.com/developers/CLOB/websocket/
    """

    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, config: Optional[Dict[str, Any]] = None, exchange=None):
        super().__init__(config)

        # Reference to parent exchange for updating mid-price cache
        self.exchange = exchange

        # Market ID to asset ID mapping
        self.market_to_asset: Dict[str, str] = {}

        # Track subscribed asset IDs
        self.subscribed_assets = set()

        # Orderbook manager
        self.orderbook_manager = OrderbookManager()

    @property
    def ws_url(self) -> str:
        """WebSocket endpoint URL for Polymarket CLOB market channel"""
        return self.WS_URL

    async def _authenticate(self):
        """
        Market channel is public, no authentication required.
        """
        if self.verbose:
            logger.debug("Market channel is public - no authentication required")

    async def _subscribe_orderbook(self, market_id: str):
        """
        Subscribe to orderbook updates for a market.

        For Polymarket, we need to subscribe using asset_id (token ID).
        The market_id is the condition_id, and we need to map it to asset_ids.

        Args:
            market_id: Market condition ID or asset ID
        """
        # Store the market_id as asset_id for subscription
        asset_id = market_id

        # Mark as subscribed
        self.subscribed_assets.add(asset_id)

        # Send subscription message
        subscribe_message = {
        "auth": {},
        "markets": [],
        "assets_ids": list(self.subscribed_assets),
        "type": "market",
        "custom_feature_enabled": True,
    }

        await self.ws.send(json.dumps(subscribe_message))

        if self.verbose:
            logger.debug(f"Subscribed to market/asset: {asset_id}")

    async def _unsubscribe_orderbook(self, market_id: str):
        """
        Unsubscribe from orderbook updates.

        Args:
            market_id: Market condition ID or asset ID
        """
        asset_id = market_id

        # Remove from subscribed set
        self.subscribed_assets.discard(asset_id)

        # Send unsubscription (resubscribe with remaining assets)
        subscribe_message = {
            "auth": {},
            "markets": [],
            "assets_ids": list(self.subscribed_assets),
            "type": "market",
        }

        await self.ws.send(json.dumps(subscribe_message))

        if self.verbose:
            logger.debug(f"Unsubscribed from market/asset: {asset_id}")

    def _parse_orderbook_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse incoming WebSocket message into standardized orderbook format.

        Handles two message types:
        1. book - Full orderbook snapshot (bids/asks arrays)
        2. price_change - Price updates with best bid/ask

        Args:
            message: Raw message from WebSocket

        Returns:
            Standardized orderbook data or None if not an orderbook message
        """
        event_type = message.get("event_type")

        if event_type == "book":
            return self._parse_book_message(message)
        elif event_type == "price_change":
            return self._parse_price_change_message(message)
        elif event_type == "tick_size_change":
            return self._parse_tick_size_change_message(message)
        elif event_type == "last_trade_price":
            return self._parse_last_trade_price_message(message)
        elif event_type == "best_bid_ask":
            return self._parse_best_bid_ask_message(message)

        return None

    def _parse_best_bid_ask_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse best_bid_ask message.
    
        Emitted when the best bid or ask price changes. Requires
        custom_feature_enabled: true in the subscription message.
    
        Message format:
        {
            "event_type": "best_bid_ask",
            "asset_id": "token_id",
            "market": "condition_id",
            "best_bid": "0.46",
            "best_ask": "0.47",
            "timestamp": "1234567890000"
        }
        """
        asset_id = message.get("asset_id", "")
        market_id = message.get("market", asset_id)
    
        try:
            best_bid_raw = message.get("best_bid")
            best_ask_raw = message.get("best_ask")
            best_bid = float(best_bid_raw) if best_bid_raw else None
            best_ask = float(best_ask_raw) if best_ask_raw else None
        except (ValueError, TypeError):
            return None
    
        if best_bid is None and best_ask is None:
            return None
    
        try:
            timestamp = int(message.get("timestamp", 0))
        except (ValueError, TypeError):
            timestamp = 0
    
        return {
            "event_type": "best_bid_ask",
            "market_id": market_id,
            "asset_id": asset_id,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "timestamp": timestamp,
        }

    def _parse_last_trade_price_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse last_trade_price message.

        Emitted when a maker and taker order is matched, creating a trade event.

        Message format:
        {
            "event_type": "last_trade_price",
            "asset_id": "token_id",
            "market": "condition_id",
            "price": "0.456",
            "side": "BUY",
            "size": "219.217767",
            "fee_rate_bps": "0",
            "timestamp": "1750428146322"
        }
        """
        asset_id = message.get("asset_id", "")
        market_id = message.get("market", asset_id)

        try:
            price = float(message.get("price", 0))
        except (ValueError, TypeError):
            price = 0.0

        try:
            size = float(message.get("size", 0))
        except (ValueError, TypeError):
            size = 0.0

        try:
            fee_rate_bps = int(message.get("fee_rate_bps", 0))
        except (ValueError, TypeError):
            fee_rate_bps = 0

        try:
            timestamp = int(message.get("timestamp", 0))
        except (ValueError, TypeError):
            timestamp = 0

        return {
            "event_type": "last_trade_price",
            "asset_id": asset_id,
            "market_id": market_id,
            "price": price,
            "side": message.get("side", ""),
            "size": size,
            "fee_rate_bps": fee_rate_bps,
            "timestamp": timestamp,
        }

    def _parse_tick_size_change_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse tick_size_change message.

        Emitted when the minimum tick size of a market changes, triggered when
        the book price crosses 0.96 (upward) or 0.04 (downward).

        Message format:
        {
            "event_type": "tick_size_change",
            "asset_id": "token_id",
            "market": "condition_id",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
            "timestamp": "1234567890000"
        }
        """
        asset_id = message.get("asset_id", "")
        market_id = message.get("market", asset_id)

        try:
            old_tick_size = float(message.get("old_tick_size", 0))
        except (ValueError, TypeError):
            old_tick_size = None

        try:
            new_tick_size = float(message.get("new_tick_size", 0))
        except (ValueError, TypeError):
            new_tick_size = None

        return {
            "market_id": market_id,
            "asset_id": asset_id,
            "old_tick_size": old_tick_size,
            "new_tick_size": new_tick_size,
            "timestamp": message.get("timestamp", 0),
            "event_type": "tick_size_change",
        }

    def _parse_book_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse book message (full orderbook snapshot).

        Message format:
        {
            "event_type": "book",
            "asset_id": "token_id",
            "market": "condition_id",
            "timestamp": 1234567890,
            "hash": "...",
            "bids": [{"price": "0.52", "size": "100"}, ...],
            "asks": [{"price": "0.53", "size": "100"}, ...]
        }
        """
        asset_id = message.get("asset_id", "")
        market_id = message.get("market", asset_id)

        # Parse bids and asks
        bids = []
        for bid in message.get("bids", []):
            try:
                price = float(bid.get("price", 0))
                size = float(bid.get("size", 0))
                if price > 0 and size > 0:
                    bids.append((price, size))
            except (ValueError, TypeError):
                continue

        asks = []
        for ask in message.get("asks", []):
            try:
                price = float(ask.get("price", 0))
                size = float(ask.get("size", 0))
                if price > 0 and size > 0:
                    asks.append((price, size))
            except (ValueError, TypeError):
                continue

        # Sort bids descending, asks ascending
        bids.sort(reverse=True)
        asks.sort()

        return {
            "market_id": market_id,
            "asset_id": asset_id,
            "bids": bids,
            "asks": asks,
            "timestamp": message.get("timestamp", 0),
            "hash": message.get("hash", ""),
        }

    def _parse_price_change_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse price_change message (incremental updates).

        Message format:
        {
            "event_type": "price_change",
            "market": "condition_id",
            "timestamp": 1234567890,
            "price_changes": [{
                "asset_id": "token_id",
                "price": "0.52",
                "size": "100",
                "side": "BUY",
                "hash": "...",
                "best_bid": "0.52",
                "best_ask": "0.53"
            }]
        }
        """
        market_id = message.get("market", "")
        timestamp = message.get("timestamp", 0)

        price_changes = message.get("price_changes", [])
        if not price_changes:
            return None

        # Use first price change (usually one per message)
        change = price_changes[0]
        asset_id = change.get("asset_id", "")

        # Build orderbook from best bid/ask
        bids = []
        asks = []

        try:
            best_bid = change.get("best_bid")
            best_ask = change.get("best_ask")

            if best_bid:
                price = float(best_bid)
                if price > 0:
                    # We don't get size from price_change, use placeholder
                    bids.append((price, 0.0))

            if best_ask:
                price = float(best_ask)
                if price > 0:
                    asks.append((price, 0.0))
        except (ValueError, TypeError):
            pass

        return {
            "market_id": market_id,
            "asset_id": asset_id,
            "bids": bids,
            "asks": asks,
            "timestamp": timestamp,
            "hash": change.get("hash", ""),
            "side": change.get("side"),
            "price": float(change.get("price", 0)) if change.get("price") else None,
            "size": float(change.get("size", 0)) if change.get("size") else None,
        }

    async def watch_orderbook_by_asset(self, asset_id: str, callback):
        """
        Subscribe to orderbook updates for a specific asset (token).

        Args:
            asset_id: Token ID to watch
            callback: Function to call with orderbook updates
        """
        await self.watch_orderbook(asset_id, callback)

    async def watch_orderbook_by_market(self, market_id: str, asset_ids: list[str], callback=None):
        """
        Subscribe to orderbook updates for a market with multiple assets.

        For binary markets, there are typically two assets (YES/NO tokens).

        Args:
            market_id: Market condition ID
            asset_ids: List of asset (token) IDs for this market
            callback: Optional function to call with orderbook updates.
                     If None, data will be stored in orderbook_manager only.
        """
        # Store mapping
        for asset_id in asset_ids:
            self.market_to_asset[market_id] = asset_id

            # Create callback that updates manager and exchange mid-price cache
            def make_callback(tid):
                def cb(market_id, orderbook):
                    # Update orderbook manager
                    self.orderbook_manager.update(tid, orderbook)
                    # Update exchange mid-price cache
                    if self.exchange:
                        self.exchange.update_mid_price_from_orderbook(tid, orderbook)
                    # Call user callback if provided
                    if callback:
                        callback(market_id, orderbook)

                return cb

            await self.watch_orderbook(asset_id, make_callback(asset_id))

    def get_orderbook_manager(self) -> OrderbookManager:
        """
        Get the orderbook manager for easy access to orderbook data.

        Returns:
            OrderbookManager instance

        Example:
            >>> ws = exchange.get_websocket()
            >>> await ws.watch_orderbook_by_market(market_id, token_ids)
            >>> ws.start()
            >>> time.sleep(2)  # Wait for data
            >>> manager = ws.get_orderbook_manager()
            >>> bid, ask = manager.get_best_bid_ask(token_id)
        """
        return self.orderbook_manager

    async def _process_message_item(self, data: dict):
        """
        Process a single message item.
        Override to handle both market_id and asset_id lookups.
        """
        try:
            # Parse orderbook data
            orderbook = self._parse_orderbook_message(data)
            if not orderbook:
                return

            # Try both market_id and asset_id as subscription keys
            market_id = orderbook.get("market_id")
            asset_id = orderbook.get("asset_id")

            # Check which key is in subscriptions
            callback = None
            callback_key = None

            if asset_id and asset_id in self.subscriptions:
                callback = self.subscriptions[asset_id]
                callback_key = asset_id
            elif market_id and market_id in self.subscriptions:
                callback = self.subscriptions[market_id]
                callback_key = market_id

            if callback and callback_key:
                # Call callback in a non-blocking way
                if asyncio.iscoroutinefunction(callback):
                    await callback(callback_key, orderbook)
                else:
                    callback(callback_key, orderbook)
        except Exception as e:
            if self.verbose:
                logger.debug(f"Error processing message item: {e}")


TradeCallback = Callable[[Trade], None]
OrderCallback = Callable[[OrderEvent], None]
MessageCallback = Callable[[Dict[str, Any]], None]


class PolymarketUserWebSocket:
    """
    Polymarket User WebSocket for real-time trade/fill notifications.

    Connects to the user channel which provides:
    - Trade events when orders are filled
    - Order status updates

    Requires API credentials for authentication.
    """

    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        verbose: bool = False,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.verbose = verbose

        self.ws = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._trade_callbacks: List[TradeCallback] = []
        self._order_callbacks: List[OrderCallback] = []
        self._message_callbacks: List[MessageCallback] = []
        self._connected = False

    def on_trade(self, callback: TradeCallback) -> "PolymarketUserWebSocket":
        """Register a callback for trade events"""
        self._trade_callbacks.append(callback)
        return self

    def on_order(self, callback: OrderCallback) -> "PolymarketUserWebSocket":
        """Register a callback for order events."""
        self._order_callbacks.append(callback)
        return self

    def on_message(self, callback: MessageCallback) -> "PolymarketUserWebSocket":
        """Register a callback for raw user-channel message items."""
        self._message_callbacks.append(callback)
        return self

    def _build_auth_message(self) -> dict:
        """Build authentication message for user WebSocket"""
        timestamp = int(time.time())

        return {
            "auth": {
                "apiKey": self.api_key,
                "secret": self.api_secret,
                "passphrase": self.api_passphrase,
                "timestamp": timestamp,
            },
            "type": "user",
        }

    async def _connect(self):
        """Connect and authenticate to WebSocket"""
        self.ws = await websockets.connect(
            self.WS_URL,
            ping_interval=20.0,
            ping_timeout=10.0,
        )
        self._connected = True

        # Send authentication
        auth_msg = self._build_auth_message()
        await self.ws.send(json.dumps(auth_msg))

        if self.verbose:
            logger.info("User WebSocket connected and authenticated")

    async def _receive_loop(self):
        """Main receive loop"""
        while self._running:
            try:
                if not self._connected:
                    await self._connect()

                async for message in self.ws:
                    if message in ("PONG", "PING", ""):
                        continue

                    try:
                        data = json.loads(message)
                        await self._handle_message(data)
                    except json.JSONDecodeError:
                        pass

            except websockets.exceptions.ConnectionClosed as e:
                if self.verbose:
                    logger.warning(f"User WebSocket closed: {e}")
                self._connected = False
                if self._running:
                    await asyncio.sleep(3)

            except Exception as e:
                if self.verbose:
                    logger.warning(f"User WebSocket error: {e}")
                self._connected = False
                if self._running:
                    await asyncio.sleep(3)

    async def _handle_message(self, data: dict):
        """Handle incoming WebSocket message"""
        if isinstance(data, list):
            for item in data:
                await self._process_item(item)
        else:
            await self._process_item(data)

    async def _process_item(self, data: dict):
        """Process a single message item"""
        self._emit_message(dict(data))

        msg_type = (data.get("type", "") or "").upper()
        event_type = (data.get("event_type", "") or "").lower()

        # Polymarket sends "type": "TRADE" for fill notifications
        if msg_type == "TRADE":
            trade = self._parse_trade(data)
            if trade and trade.size > 0:
                self._emit_trade(trade)
            return

        if event_type == "order":
            order = self._parse_order(data)
            if order:
                self._emit_order(order)

    def _parse_trade(self, data: dict) -> Optional[Trade]:
        """Parse TRADE message from Polymarket user WebSocket"""
        try:
            # Parse matchtime (unix timestamp in seconds) - note: no underscore!
            ts = data.get("matchtime") or data.get("match_time", 0)
            if isinstance(ts, str):
                ts = int(ts)

            # Parse last_update timestamp
            last_update = data.get("last_update", ts)
            if isinstance(last_update, str):
                last_update = int(last_update)

            # Parse maker_orders into MakerOrder objects
            maker_orders_raw = data.get("maker_orders", [])
            maker_orders = []
            if maker_orders_raw:
                for mo in maker_orders_raw:
                    try:
                        maker_orders.append(MakerOrder(
                            order_id=mo.get("order_id", ""),
                            owner=mo.get("owner", ""),
                            maker_address=mo.get("maker_address", ""),
                            matched_amount=float(mo.get("matched_amount", 0)),
                            price=float(mo.get("price", 0)),
                            fee_rate_bps=int(mo.get("fee_rate_bps", 0)),
                            asset_id=mo.get("asset_id", ""),
                            outcome=mo.get("outcome", ""),
                            side=mo.get("side", "")
                        ))
                    except Exception as e:
                        if self.verbose:
                            logger.warning(f"Failed to parse maker_order: {e}")

            return Trade(
                event_type=data.get("event_type", ""),
                type=data.get("type", ""),
                id=data.get("id", ""),
                taker_order_id=data.get("taker_order_id", ""),
                order_id=data.get("taker_order_id", ""),
                market_id=data.get("market", ""),
                asset_id=data.get("asset_id", ""),
                side=data.get("side", ""),
                size=float(data.get("size", 0)),
                price=float(data.get("price", 0)),
                fee_rate_bps=int(data.get("fee_rate_bps", 0)),
                status=data.get("status", ""),
                match_time=data.get("matchtime", ""),
                last_update=data.get("last_update", ""),
                outcome=data.get("outcome", ""),
                owner=data.get("owner", ""),
                trade_owner=data.get("trade_owner", ""),
                maker_address=data.get("maker_address", ""),
                transaction_hash=data.get("transaction_hash", ""),
                bucket_index=int(data.get("bucket_index", 0)),
                maker_orders=maker_orders,
                trader_side=data.get("trader_side", ""),
                taker=data.get("taker_order_id", ""),
                maker=data.get("maker_order_id", ""),
                timestamp=data.get("timestamp", ""),
                raw_event=dict(data),
            )
        except Exception as e:
            if self.verbose:
                logger.warning(f"Failed to parse trade: {e}")
            return None

    def _parse_order(self, data: dict) -> Optional[OrderEvent]:
        """Parse order message from Polymarket user WebSocket."""
        try:
            return OrderEvent(
                event_type=data.get("event_type", "order"),
                type=data.get("type", ""),
                id=data.get("id", ""),
                order_id=data.get("id", ""),
                owner=data.get("owner", ""),
                market_id=data.get("market", ""),
                market=data.get("market", ""),
                asset_id=data.get("asset_id", ""),
                side=data.get("side", ""),
                order_owner=data.get("order_owner", ""),
                original_size=float(data.get("original_size", 0)),
                size_matched=float(data.get("size_matched", 0)),
                price=float(data.get("price", 0)),
                associate_trades=data.get("associate_trades"),
                outcome=data.get("outcome", ""),
                created_at=data.get("created_at", ""),
                expiration=data.get("expiration", ""),
                order_type=data.get("order_type", ""),
                status=data.get("status", ""),
                maker_address=data.get("maker_address", ""),
                timestamp=data.get("timestamp", ""),
                raw_event=dict(data),
            )
        except Exception as e:
            if self.verbose:
                logger.warning(f"Failed to parse order update: {e}")
            return None

    def _emit_trade(self, trade: Trade):
        """Emit trade to all callbacks"""
        for callback in self._trade_callbacks:
            try:
                callback(trade)
            except Exception as e:
                if self.verbose:
                    logger.warning(f"Trade callback error: {e}")

    def _emit_order(self, order: OrderEvent):
        """Emit order update to all callbacks."""
        for callback in self._order_callbacks:
            try:
                callback(order)
            except Exception as e:
                if self.verbose:
                    logger.warning(f"Order callback error: {e}")

    def _emit_message(self, data: Dict[str, Any]):
        """Emit raw message item to all callbacks."""
        for callback in self._message_callbacks:
            try:
                callback(data)
            except Exception as e:
                if self.verbose:
                    logger.warning(f"Message callback error: {e}")

    def start(self) -> threading.Thread:
        """Start WebSocket in background thread"""
        if self._running:
            return self._thread

        self._running = True
        self._loop = asyncio.new_event_loop()

        def run():
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._receive_loop())

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

        if self.verbose:
            logger.info("User WebSocket started")

        return self._thread

    def stop(self):
        """Stop WebSocket"""
        self._running = False

        if self.ws and self._loop:

            async def close():
                if self.ws:
                    await self.ws.close()

            asyncio.run_coroutine_threadsafe(close(), self._loop)

        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

        if self.verbose:
            logger.info("User WebSocket stopped")
