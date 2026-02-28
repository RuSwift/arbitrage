/**
 * KLines — Vue 2 UMD component: свечной график + объём (TradingView/Binance style).
 * Delimiters [[ ]] to avoid Jinja2 conflict.
 * Props: klines, token, symbol, selectedVolumeCandleIndex. Emits: volume-bar-click(originalIdx).
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['vue'], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(require('vue'));
    } else {
        root.KLines = factory(root.Vue);
    }
}(typeof self !== 'undefined' ? self : this, function (Vue) {

    Vue.component('klines-chart', {
        delimiters: ['[[', ']]'],
        props: {
            klines: { type: Array, default: function () { return []; } },
            token: { type: String, default: '' },
            symbol: { type: String, default: '' },
            selectedVolumeCandleIndex: { type: Number, default: null }
        },
        methods: {
            klinesScale: function (klines) {
                if (!klines || !klines.length) return { min: 0, max: 1 };
                var low = Infinity, high = -Infinity;
                klines.forEach(function (c) {
                    if (c.low_price != null && c.low_price < low) low = c.low_price;
                    if (c.high_price != null && c.high_price > high) high = c.high_price;
                });
                if (low === Infinity) low = 0;
                if (high <= low) high = low + 1;
                return { min: low, max: high };
            },
            candleWickStyle: function (c) {
                var klines = this.klines;
                if (!klines || !klines.length) return {};
                var scale = this.klinesScale(klines);
                var range = scale.max - scale.min;
                if (range === 0) return {};
                var lowPct = ((c.low_price - scale.min) / range) * 100;
                var highPct = ((c.high_price - scale.min) / range) * 100;
                var height = Math.max(0.5, highPct - lowPct);
                return {
                    height: height + '%',
                    bottom: lowPct + '%',
                    width: '1px'
                };
            },
            candleBodyStyle: function (c, idx) {
                var klines = this.klines;
                if (!klines || !klines.length) return {};
                var scale = this.klinesScale(klines);
                var range = scale.max - scale.min;
                if (range === 0) return {};
                var openPct = ((c.open_price - scale.min) / range) * 100;
                var closePct = ((c.close_price - scale.min) / range) * 100;
                var bottom = Math.min(openPct, closePct);
                var height = Math.max(1, Math.abs(closePct - openPct));
                return {
                    height: height + '%',
                    bottom: bottom + '%',
                    width: '100%'
                };
            },
            candleColStyle: function (idx) {
                var klines = this.klines;
                if (!klines || !klines.length) return {};
                var w = (100 / klines.length);
                return {
                    left: (idx / klines.length) * 100 + '%',
                    width: Math.max(1.5, w * 0.9) + '%'
                };
            },
            formatKlineTime: function (utc) {
                if (utc == null) return '—';
                var d = new Date(Number(utc) * 1000);
                if (isNaN(d.getTime())) return '—';
                return d.toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
            },
            formatKlineTooltip: function (c) {
                if (!c) return '';
                var s = 'O:' + c.open_price + ' H:' + c.high_price + ' L:' + c.low_price + ' C:' + c.close_price;
                var vol = c.coin_volume != null ? Number(c.coin_volume) : 0;
                var volUsd = c.usd_volume != null ? Number(c.usd_volume) : null;
                if (vol > 0 || (volUsd != null && volUsd > 0)) {
                    s += ' | Vol: ' + (vol.toFixed ? vol.toFixed(4) : vol);
                    if (volUsd != null && volUsd > 0) s += ' | $: ' + this.formatUsd(volUsd);
                }
                return s;
            },
            volumeBarStyle: function (c, idx) {
                var klines = this.klines;
                if (!klines || !klines.length) return {};
                var maxVol = this.klinesVolumeMax;
                if (maxVol <= 0) return this.candleColStyle(idx);
                var vol = c.usd_volume != null && c.usd_volume > 0 ? Number(c.usd_volume) : Number(c.coin_volume || 0);
                var hPct = maxVol > 0 ? (vol / maxVol) * 100 : 0;
                var style = this.candleColStyle(idx);
                style.height = Math.max(2, hPct) + '%';
                style.bottom = '0';
                return style;
            },
            priceTickBottomPct: function (tick) {
                var k = this.klines;
                if (!k || !k.length) return 0;
                var scale = this.klinesScale(k);
                var range = scale.max - scale.min;
                if (range <= 0) return 0;
                return ((Number(tick) - scale.min) / range) * 100;
            },
            volumeTickBottomPct: function (tick) {
                var maxVol = this.klinesVolumeMax;
                if (maxVol <= 0) return 0;
                return (Number(tick) / maxVol) * 100;
            },
            formatVolumeShort: function (val) {
                if (val == null || isNaN(val) || val === 0) return '0';
                var n = Number(val);
                if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
                if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
                return n.toFixed(2);
            },
            getVolumeLabel: function (c) {
                if (!c) return '';
                var vol = c.coin_volume != null ? Number(c.coin_volume) : 0;
                var volUsd = c.usd_volume != null ? Number(c.usd_volume) : null;
                var s = 'Vol: ' + (vol.toFixed ? vol.toFixed(4) : vol);
                if (volUsd != null && volUsd > 0) s += ' | $: ' + this.formatUsd(volUsd);
                s += ' | ' + this.formatKlineTime(c.utc_open_time);
                return s;
            },
            getOriginalKlineIndex: function (displayIdx) {
                var k = this.klines;
                return k && k.length ? k.length - 1 - displayIdx : 0;
            },
            onVolumeBarClick: function (c, displayIdx) {
                this.$emit('volume-bar-click', this.getOriginalKlineIndex(displayIdx));
            },
            formatUsd: function (val) {
                if (val == null || isNaN(val)) return '—';
                return Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            },
            formatOrderPrice: function (p) {
                if (p == null || isNaN(p)) return '—';
                return Number(p).toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
            }
        },
        computed: {
            klinesForChart: function () {
                var k = this.klines;
                if (!k || !k.length) return [];
                return k.slice().reverse();
            },
            klinesPriceTicks: function () {
                var k = this.klines;
                if (!k || !k.length) return [];
                var scale = this.klinesScale(k);
                var range = scale.max - scale.min;
                if (range <= 0) return [scale.min];
                var ticks = [];
                var n = 5;
                for (var i = 0; i <= n; i++) ticks.push(scale.min + (range * i / n));
                return ticks.reverse();
            },
            klinesTimeLabels: function () {
                var k = this.klinesForChart;
                if (!k || !k.length) return [];
                var len = k.length;
                if (len === 1) return [{ index: 0, label: this.formatKlineTime(k[0].utc_open_time), leftPct: 0 }];
                var step = Math.max(1, Math.floor(len / 6));
                var out = [];
                for (var i = 0; i < len; i += step) {
                    out.push({ index: i, label: this.formatKlineTime(k[i].utc_open_time), leftPct: (i / (len - 1)) * 100 });
                }
                if (len - 1 - (out[out.length - 1].index) > step * 0.5) out.push({ index: len - 1, label: this.formatKlineTime(k[len - 1].utc_open_time), leftPct: 100 });
                return out;
            },
            klinesVolumeMax: function () {
                var k = this.klines;
                if (!k || !k.length) return 0;
                var max = 0;
                k.forEach(function (c) {
                    var v = c.usd_volume != null && c.usd_volume > 0 ? Number(c.usd_volume) : Number(c.coin_volume || 0);
                    if (v > max) max = v;
                });
                return max;
            },
            klinesVolumeTicks: function () {
                var maxVol = this.klinesVolumeMax;
                if (maxVol <= 0) return [0];
                var ticks = [];
                var n = 4;
                for (var i = 0; i <= n; i++) ticks.push((maxVol * i / n));
                return ticks.reverse();
            }
        },
        template:
            '<div v-if="klines && klines.length" class="klines-binance mb-4">' +
            '  <div class="klines-header">' +
            '    <span class="klines-title"><i class="bi bi-bar-chart-line me-1"></i>KLines</span>' +
            '    <span class="klines-symbol">[[ token || symbol || \'\' ]]</span>' +
            '    <span v-if="selectedVolumeCandleIndex != null && klines[selectedVolumeCandleIndex]" class="klines-volume-selected ms-2 text-nowrap" :title="getVolumeLabel(klines[selectedVolumeCandleIndex])">[[ getVolumeLabel(klines[selectedVolumeCandleIndex]) ]]</span>' +
            '  </div>' +
            '  <div class="klines-body">' +
            '    <div class="klines-row">' +
            '      <div class="klines-chart-wrap">' +
            '        <div class="klines-candles">' +
            '          <div v-for="(c, idx) in klinesForChart" :key="idx" class="candle-col" :style="candleColStyle(idx)" :title="formatKlineTooltip(c)">' +
            '            <div class="candle-wick" :style="candleWickStyle(c)"></div>' +
            '            <div class="candle-body" :class="c.close_price >= c.open_price ? \'candle-up\' : \'candle-down\'" :style="candleBodyStyle(c, idx)"></div>' +
            '          </div>' +
            '        </div>' +
            '      </div>' +
            '      <div class="klines-price-axis">' +
            '        <div v-for="(tick, i) in klinesPriceTicks" :key="\'p\'+i" class="klines-price-tick" :style="{ bottom: priceTickBottomPct(tick) + \'%\' }">[[ formatOrderPrice(tick) ]]</div>' +
            '      </div>' +
            '    </div>' +
            '    <div class="klines-volume-row">' +
            '      <div class="klines-volume-wrap">' +
            '        <div class="klines-volume-bars">' +
            '          <div v-for="(c, idx) in klinesForChart" :key="\'v\'+idx" class="volume-bar-col" :class="{ \'volume-bar-selected\': selectedVolumeCandleIndex === getOriginalKlineIndex(idx) }" :style="volumeBarStyle(c, idx)" @click.stop="onVolumeBarClick(c, idx)">' +
            '            <div class="volume-bar" :class="c.close_price >= c.open_price ? \'volume-up\' : \'volume-down\'"></div>' +
            '          </div>' +
            '        </div>' +
            '      </div>' +
            '      <div class="klines-volume-axis">' +
            '        <div v-for="(tick, i) in klinesVolumeTicks" :key="\'vt\'+i" class="klines-volume-tick" :style="{ bottom: volumeTickBottomPct(tick) + \'%\' }">[[ formatVolumeShort(tick) ]]</div>' +
            '      </div>' +
            '    </div>' +
            '    <div class="klines-time-row">' +
            '      <div class="klines-time-axis">' +
            '        <div v-for="(item, i) in klinesTimeLabels" :key="\'t\'+i" class="klines-time-tick" :style="{ left: item.leftPct + \'%\' }">[[ item.label ]]</div>' +
            '      </div>' +
            '      <div class="klines-time-axis-spacer"></div>' +
            '    </div>' +
            '  </div>' +
            '</div>'
    });

    return Vue;
}));
