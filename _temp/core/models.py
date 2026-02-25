import hashlib
from typing import Set, Tuple

from django.db import models
from django.contrib.postgres.fields import ArrayField
from django.contrib.auth.models import AbstractUser

from core.utils import utc_now_float


class UserAccount(AbstractUser):

    is_admin = models.BooleanField(default=True)

    @property
    def user_id(self) -> str:
        return self.username

    @user_id.setter
    def user_id(self, value: str):
        self.username = value

    class Meta(AbstractUser.Meta):
        swappable = 'AUTH_USER_MODEL'


class UserAccountCredential(models.Model):
    engine = models.CharField(max_length=128, db_index=True)
    account = models.ForeignKey(
        UserAccount, on_delete=models.CASCADE, related_name='credentials'
    )
    secrets = models.JSONField()

    class Meta:
        unique_together = [('engine', 'account')]


class Session(models.Model):
    session_id = models.CharField(max_length=128, db_index=True, unique=True)
    account = models.ForeignKey(
        UserAccount, on_delete=models.CASCADE, related_name='sessions'
    )
    credential = models.ForeignKey(
        UserAccountCredential, on_delete=models.CASCADE,
        related_name='sessions', null=True
    )
    storage = models.JSONField()


class P2PAdvertiser(models.Model):
    """Рекламодатель"""
    exchange_code = models.CharField(max_length=24, db_index=True)
    uid = models.CharField(max_length=64, db_index=True)
    nick = models.CharField(max_length=64, db_index=True)
    rating = models.FloatField(null=True, db_index=True)
    finish_rate = models.FloatField(null=True, db_index=True)
    trades_month = models.IntegerField(null=True, db_index=True)
    trades_total = models.IntegerField(null=True, db_index=True)
    hash = models.TextField()
    extra = models.JSONField()
    verified_merchant = models.BooleanField(null=True)
    recommended = models.BooleanField(default=False)

    class Meta:
        unique_together = [('exchange_code', 'uid')]


class P2PAdvertInfo(models.Model):
    """Уникальное объявление"""
    advertiser = models.ForeignKey(P2PAdvertiser, on_delete=models.SET_NULL, null=True)
    uid = models.CharField(max_length=128, db_index=True)
    price = models.FloatField()
    quantity = models.FloatField()
    quantity_fiat = models.FloatField(null=True)
    min_qty = models.FloatField()
    max_qty = models.FloatField()
    pay_methods = ArrayField(models.CharField(max_length=24), db_index=True, default=list)
    pay_methods_hash = models.CharField(max_length=64, db_index=True, null=True)
    # эпоха прохода по объявлениям
    epoch = models.FloatField(db_index=True)
    # в течение след эпох объявление могло не меняться
    duplicate_epochs = ArrayField(models.FloatField(), default=list)
    extra = models.JSONField(null=True)
    data_hash = models.TextField(db_index=True)
    fiat = models.CharField(max_length=10, db_index=True, null=True)
    exchange_code = models.CharField(max_length=24, db_index=True, null=True)
    side = models.CharField(max_length=16, db_index=True, null=True)
    token = models.CharField(max_length=16, db_index=True, null=True)


class P2PAdvertPosition(models.Model):
    adv = models.ForeignKey(P2PAdvertInfo, on_delete=models.CASCADE, null=True, related_name='positions')
    epoch = models.FloatField(db_index=True, null=True)
    position = models.IntegerField(null=True)
    created = models.FloatField(default=utc_now_float)
    fiat = models.CharField(max_length=10, db_index=True, null=True)
    exchange_code = models.CharField(max_length=24, db_index=True, null=True)
    side = models.CharField(max_length=16, db_index=True, null=True)
    token = models.CharField(max_length=16, db_index=True, null=True)


class P2PEpoch(models.Model):
    epoch = models.FloatField(db_index=True)
    exchange_code = models.CharField(max_length=24, db_index=True, null=True)
    side = models.CharField(max_length=16, db_index=True, null=True)
    token = models.CharField(max_length=16, db_index=True, null=True)
    fiat = models.CharField(max_length=10, db_index=True, null=True)
    errors = models.JSONField(null=True)

    class Meta:
        unique_together = [('epoch', 'exchange_code', 'side', 'token')]

    def calc_hash(self) -> str:
        s = f'{self.epoch}/{self.exchange_code}/{self.side}/{self.token}'
        return hashlib.md5(s.encode()).hexdigest()


class P2PTradeEvent(models.Model):
    uid = models.CharField(max_length=48, unique=True, null=True)
    epoch = models.FloatField(db_index=True)
    exchange_code = models.CharField(max_length=24, db_index=True, null=True)
    side = models.CharField(max_length=16, db_index=True, null=True)
    token = models.CharField(max_length=16, db_index=True, null=True)
    fiat = models.CharField(max_length=10, db_index=True, null=True)
    # uid объявления
    advert_uid = models.CharField(max_length=128, db_index=True)
    advertiser_uid = models.CharField(max_length=128, db_index=True, null=True)
    position = models.IntegerField(db_index=True, null=True)
    counterparty_position = models.IntegerField(db_index=True, null=True)
    # цена сделки
    price = models.FloatField()
    # размер сделки
    quantity = models.FloatField()
    extra = models.JSONField(null=True)
    # платежные методы
    pay_methods = ArrayField(models.CharField(max_length=24), db_index=True, default=list)
    pay_methods_hash = models.CharField(max_length=64, db_index=True, null=True)
    usdt_price = models.FloatField(null=True)
    forex_ratio = models.FloatField(null=True)


class ExchangeCollectorsStorage(models.Model):
    exchange_code = models.CharField(max_length=32, db_index=True)
    storage = models.JSONField()
    updated = models.FloatField()


class ExchangeCollectorItem(models.Model):
    exchange = models.CharField(max_length=36, db_index=True)
    uid = models.CharField(max_length=128, db_index=True, null=True)
    timestamp = models.FloatField(db_index=True)
    data = models.JSONField()
    tags = ArrayField(models.CharField(max_length=24), db_index=True, default=list)


class SpreadEvent(models.Model):
    symbol = models.CharField(max_length=24, db_index=True)
    utc = models.FloatField(db_index=True)
    ex_buy = models.CharField(max_length=36, db_index=True)
    ex_sell = models.CharField(max_length=36, db_index=True)
    details = models.JSONField()


class SpreadRow(models.Model):
    symbol = models.CharField(max_length=24, db_index=True)
    utc = models.FloatField(db_index=True)
    details = models.JSONField()
