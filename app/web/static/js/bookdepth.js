/**
 * BookDepth — Vue 2 UMD component: красный/зелёный стакан (bids/asks).
 * Delimiters [[ ]] to avoid Jinja2 conflict.
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['vue'], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(require('vue'));
    } else {
        root.BookDepth = factory(root.Vue);
    }
}(typeof self !== 'undefined' ? self : this, function (Vue) {

    Vue.component('book-depth', {
        delimiters: ['[[', ']]'],
        props: {
            bookDepth: { type: Object, default: null },
            token: { type: String, default: '' }
        },
        methods: {
            depthPctBid: function (b) {
                var bd = this.bookDepth;
                var bids = (bd && bd.bids) ? bd.bids.slice(0, 20) : [];
                if (!bids.length) return 0;
                var maxUsd = Math.max.apply(null, bids.map(function (x) { return Number(x.price) * Number(x.quantity); }));
                if (maxUsd <= 0) return 0;
                return Math.min(100, ((Number(b.price) * Number(b.quantity)) / maxUsd) * 100);
            },
            depthPctAsk: function (a) {
                var bd = this.bookDepth;
                var asks = (bd && bd.asks) ? bd.asks.slice(0, 20) : [];
                if (!asks.length) return 0;
                var maxUsd = Math.max.apply(null, asks.map(function (x) { return Number(x.price) * Number(x.quantity); }));
                if (maxUsd <= 0) return 0;
                return Math.min(100, ((Number(a.price) * Number(a.quantity)) / maxUsd) * 100);
            },
            bookDepthSpreadPct: function (rowIndex) {
                var bd = this.bookDepth;
                var bids = (bd && bd.bids) ? bd.bids.slice(0, 20) : [];
                var asks = (bd && bd.asks) ? bd.asks.slice(0, 20) : [];
                if (rowIndex >= bids.length || rowIndex >= asks.length) return '—';
                var bidPrice = Number(bids[rowIndex].price);
                var askPrice = Number(asks[rowIndex].price);
                if (!bidPrice || !askPrice || askPrice <= bidPrice) return '—';
                var mid = (bidPrice + askPrice) / 2;
                var spreadPct = ((askPrice - bidPrice) / mid) * 100;
                return spreadPct.toFixed(2) + '%';
            },
            formatOrderPrice: function (p) {
                if (p == null || isNaN(p)) return '—';
                return Number(p).toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
            },
            formatUsd: function (val) {
                if (val == null || isNaN(val)) return '—';
                return Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            },
            bookDepthTimestamp: function () {
                var bd = this.bookDepth;
                if (!bd || bd.utc == null) return '—';
                var sec = Number(bd.utc);
                if (isNaN(sec)) return '—';
                var d = new Date(sec * 1000);
                return d.toISOString ? d.toISOString() : d.toLocaleString();
            }
        },
        template:
            '<div v-if="bookDepth" class="order-book-binance mb-4">' +
            '  <div class="order-book-timestamp small mb-2" style="color: #b0b0b0;">timestamp BookDepth: [[ bookDepthTimestamp() ]]</div>' +
            '  <div class="order-book-header">' +
            '    <span class="order-book-title"><i class="bi bi-graph-up me-1"></i>Order Book</span>' +
            '    <span class="order-book-symbol">[[ bookDepth.symbol || token || \'\' ]]</span>' +
            '  </div>' +
            '  <div class="order-book-columns">' +
            '    <div class="order-book-col order-book-bids">' +
            '      <table class="order-book-table"><thead><tr><th class="order-book-combo-th"><span class="ob-price-label">PRICE</span><span class="ob-usd-label">$</span></th></tr></thead><tbody>' +
            '        <tr v-for="(b, i) in (bookDepth.bids || []).slice(0, 20)" :key="\'b\'+i">' +
            '          <td class="order-book-combo-cell bid-price"><div class="depth-bar bid-bar" :style="{ width: depthPctBid(b) + \'%\' }"></div><span class="ob-price">[[ formatOrderPrice(b.price) ]]</span><span class="ob-usd">[[ formatUsd(Number(b.price) * Number(b.quantity)) ]]</span></td>' +
            '        </tr></tbody></table>' +
            '    </div>' +
            '    <div class="order-book-divider">' +
            '      <table class="order-book-table order-book-divider-table"><thead><tr><th>Δ%</th></tr></thead><tbody>' +
            '        <tr v-for="(n, i) in 20" :key="\'d\'+i"><td class="order-book-pct-cell">[[ bookDepthSpreadPct(i) ]]</td></tr>' +
            '      </tbody></table>' +
            '    </div>' +
            '    <div class="order-book-col order-book-asks order-book-asks-mirrored">' +
            '      <table class="order-book-table"><thead><tr><th class="order-book-combo-th"><span class="ob-price-label">PRICE</span><span class="ob-usd-label">$</span></th></tr></thead><tbody>' +
            '        <tr v-for="(a, i) in (bookDepth.asks || []).slice(0, 20)" :key="\'a\'+i">' +
            '          <td class="order-book-combo-cell ask-price"><div class="depth-bar ask-bar ask-bar-mirrored" :style="{ width: depthPctAsk(a) + \'%\' }"></div><span class="ob-price">[[ formatOrderPrice(a.price) ]]</span><span class="ob-usd">[[ formatUsd(Number(a.price) * Number(a.quantity)) ]]</span></td>' +
            '        </tr></tbody></table>' +
            '    </div>' +
            '  </div>' +
            '</div>'
    });

    return Vue;
}));
