import dataclasses
from typing import List, Union, Optional, Dict

from channels.db import database_sync_to_async as sync_to_async
from django.db import transaction

from core.crud.base import CRUD
from core.utils import utc_now_float
from core.crud.resources.stats import StatItem
from core.models import ExchangeCollectorItem as DBExchangeCollectorItem


class CRUDStatsInterface(CRUD):

    @dataclasses.dataclass
    class Filters:

        @dataclasses.dataclass
        class TimeStampRange:
            begin: float = None
            end: float = None
            exact: float = None
            neq: bool = False

            def empty(self) -> bool:
                return self.begin is None and self.end is None and \
                       self.exact is None

        tags: List[str] = None
        order_by: List[str] = dataclasses.field(
            default_factory=lambda: ['pk']
        )
        time_range: TimeStampRange = None

    async def stats_append(
        self, exchange: str, item: Union[StatItem, List[StatItem]]
    ):
        items_ = item if isinstance(item, list) else [item]
        await sync_to_async(self.__append_stats_items)(exchange, items_)

    async def stats_fetch(self, exchange: str, filters: Filters) -> List[StatItem]:
        items = await sync_to_async(self.__fetch_items)(
            exchange, filters
        )
        return items

    async def stats_load(self, exchange: str, uid: str) -> Optional[Dict]:
        return await sync_to_async(self.__stats_load)(exchange, uid)

    async def stats_replace(
        self, exchange: str, uid: str, data: Dict,
        replace_timestamp: bool = True
    ):
        await sync_to_async(self.__stats_replace)(
            exchange, uid, data, replace_timestamp
        )

    async def stats_delete(self, exchange: str, uid: str):
        await sync_to_async(self.__stats_delete)(exchange, uid)

    async def stats_last(
        self, exchange: str, filters: Filters
    ) -> Optional[StatItem]:
        order_by = ['-pk'] + [
            order for order in filters.order_by if order != 'pk'
        ]
        new_filters = self.Filters(
            tags=filters.tags,
            order_by=order_by,
            time_range=filters.time_range
        )
        items = await sync_to_async(self.__fetch_items)(
            exchange, new_filters, limit=1
        )
        return items[0] if items else None

    @classmethod
    def __fetch_items(
        cls, exchange: str, filters: Filters, limit: int = None
    ) -> List[StatItem]:
        queryset = DBExchangeCollectorItem.objects
        kwargs = dict(exchange=exchange)
        if filters.tags:
            kwargs['tags__contains'] = filters.tags
        if filters.time_range and not filters.time_range.empty():
            if filters.time_range.exact:
                kwargs['timestamp'] = filters.time_range.exact
            if filters.time_range.begin:
                k = 'timestamp__gt' if filters.time_range.neq else 'timestamp__gte'
                kwargs[k] = filters.time_range.begin
            if filters.time_range.end:
                k = 'timestamp__lt' if filters.time_range.neq else 'timestamp__lte'
                kwargs[k] = filters.time_range.end
        queryset = queryset.filter(**kwargs)
        if filters.order_by:
            queryset = queryset.order_by(*filters.order_by)
        results = []
        for rec in queryset.all()[:limit]:
            results.append(
                StatItem(
                    pk=rec.pk,
                    timestamp=rec.timestamp,
                    tags=list(rec.tags),
                    uid=rec.uid,
                    data=rec.data
                )
            )
        return results

    @classmethod
    def __stats_load(cls, exchange: str, uid: str) -> Optional[Dict]:
        queryset = DBExchangeCollectorItem.objects.filter(
            exchange=exchange,
            uid=uid
        )
        rec: DBExchangeCollectorItem = queryset.first()
        if rec:
            return rec.data
        else:
            return None

    @classmethod
    def __stats_replace(
        cls, exchange: str, uid: str, data: Dict,
        replace_timestamp: bool
    ):
        with transaction.atomic():
            queryset = DBExchangeCollectorItem.objects.filter(
                exchange=exchange,
                uid=uid
            )
            rec: DBExchangeCollectorItem = queryset.first()
            if rec:
                rec.data = data
                if replace_timestamp:
                    rec.timestamp = utc_now_float()
                rec.save()
            else:
                DBExchangeCollectorItem.objects.create(
                    exchange=exchange,
                    uid=uid,
                    timestamp=utc_now_float(),
                    data=data
                )

    @classmethod
    def __stats_delete(cls, exchange: str, uid: str):
        with transaction.atomic():
            queryset = DBExchangeCollectorItem.objects.filter(
                exchange=exchange,
                uid=uid
            )
            queryset.delete()

    @classmethod
    def __append_stats_items(cls, exchange: str, items: List[StatItem]):
        with transaction.atomic():
            for item in items:
                DBExchangeCollectorItem.objects.create(
                    exchange=exchange,
                    uid=item.uid,
                    timestamp=item.timestamp,
                    data=item.data,
                    tags=item.tags
                )
