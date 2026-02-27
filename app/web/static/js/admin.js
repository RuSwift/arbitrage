/**
 * Crawler Admin — Vue 2 UMD component for CrawlerJob / CrawlerIteration analysis.
 * Delimiters [[ ]] to avoid Jinja2 conflict.
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['vue'], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(require('vue'));
    } else {
        root.CrawlerAdmin = factory(root.Vue);
    }
}(typeof self !== 'undefined' ? self : this, function (Vue) {

    // Modal Window (garantex-style)
    Vue.component('modal-window', {
        delimiters: ['[[', ']]'],
        props: {
            width: { type: String, default: '80%' },
            height: { type: String, default: '90%' }
        },
        data: function () {
            return {
                size: { width: this.width, height: this.height }
            };
        },
        created: function () {
            if (window.innerWidth < 768) this.size.width = '100%';
            else this.size.width = this.width;
            this.size.height = this.height;
        },
        template:
            '<transition name="modal">' +
            '  <div class="modal-mask" v-if="$slots.header || $slots.body || $slots.footer">' +
            '    <div class="modal-wrapper">' +
            '      <div class="modal-container" :style="{ width: size.width, height: size.height }">' +
            '        <div class="modal-header" v-if="$slots.header"><slot name="header"></slot></div>' +
            '        <div class="modal-body" v-if="$slots.body"><slot name="body"></slot></div>' +
            '        <div class="modal-footer" v-if="$slots.footer"><slot name="footer"></slot></div>' +
            '      </div>' +
            '    </div>' +
            '  </div>' +
            '</transition>'
    });

    Vue.component('crawler-admin', {
        delimiters: ['[[', ']]'],
        data: function () {
            return {
                loading: true,
                error: null,
                stats: null,
                jobs: [],
                totalJobs: 0,
                page: 1,
                pageSize: 20,
                exchangeFilter: '',
                connectorFilter: '',
                selectedJob: null,
                jobIterations: [],
                iterationsTotal: 0,
                iterationsPage: 1,
                iterationsPageSize: 50,
                statusFilter: '',
                tokenSearchQuery: '',
                selectedIteration: null,
                showIterationModal: false,
                loadingIterationDetail: false,
                selectedVolumeCandleIndex: null,
                exchanges: [],
                connectors: ['spot', 'perpetual']
            };
        },
        mounted: function () {
            this.loadStats();
            this.loadJobs();
        },
        methods: {
            loadStats: function () {
                var self = this;
                fetch('/api/admin/crawler/stats')
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.stats = data;
                        var exMap = {};
                        (data.by_exchange || []).forEach(function (x) { exMap[x.exchange] = true; });
                        self.exchanges = Object.keys(exMap).sort();
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки статистики';
                    });
            },
            loadJobs: function () {
                var self = this;
                self.loading = true;
                self.error = null;
                var params = new URLSearchParams({
                    page: self.page,
                    page_size: self.pageSize
                });
                if (self.exchangeFilter) params.set('exchange', self.exchangeFilter);
                if (self.connectorFilter) params.set('connector', self.connectorFilter);
                fetch('/api/admin/crawler/jobs?' + params)
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.jobs = data.jobs || [];
                        self.totalJobs = data.total || 0;
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки списка заданий';
                    })
                    .finally(function () { self.loading = false; });
            },
            selectJob: function (job) {
                var self = this;
                self.selectedJob = job;
                self.jobIterations = [];
                self.iterationsTotal = 0;
                self.iterationsPage = 1;
                self.loadIterations();
            },
            loadIterations: function () {
                if (!this.selectedJob) return;
                var self = this;
                var params = new URLSearchParams({
                    page: self.iterationsPage,
                    page_size: self.iterationsPageSize
                });
                if (self.statusFilter) params.set('status', self.statusFilter);
                fetch('/api/admin/crawler/jobs/' + self.selectedJob.id + '/iterations?' + params)
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.jobIterations = data.iterations || [];
                        self.iterationsTotal = data.total || 0;
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки итераций';
                    });
            },
            showIteration: function (it) {
                var self = this;
                self.selectedIteration = null;
                self.showIterationModal = true;
                self.loadingIterationDetail = true;
                fetch('/api/admin/crawler/iterations/' + it.id)
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.selectedIteration = data;
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки итерации';
                    })
                    .finally(function () { self.loadingIterationDetail = false; });
            },
            closeJobPanel: function () {
                this.selectedJob = null;
                this.jobIterations = [];
            },
            closeIterationModal: function () {
                this.showIterationModal = false;
                this.selectedIteration = null;
                this.selectedVolumeCandleIndex = null;
            },
            formatDate: function (s) {
                if (!s) return '—';
                try {
                    var d = new Date(s);
                    return d.toLocaleString('ru-RU');
                } catch (e) { return s; }
            },
            formatUsd: function (val) {
                if (val == null || isNaN(val)) return '—';
                return Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            },
            formatOrderPrice: function (p) {
                if (p == null || isNaN(p)) return '—';
                return Number(p).toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
            },
            formatOrderAmount: function (q) {
                if (q == null || isNaN(q)) return '—';
                return Number(q).toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 8 });
            },
            depthPctBid: function (b) {
                var bd = this.selectedIteration && this.selectedIteration.book_depth;
                var bids = (bd && bd.bids) ? bd.bids.slice(0, 20) : [];
                if (!bids.length) return 0;
                var maxUsd = Math.max.apply(null, bids.map(function (x) { return Number(x.price) * Number(x.quantity); }));
                if (maxUsd <= 0) return 0;
                return Math.min(100, ((Number(b.price) * Number(b.quantity)) / maxUsd) * 100);
            },
            depthPctAsk: function (a) {
                var bd = this.selectedIteration && this.selectedIteration.book_depth;
                var asks = (bd && bd.asks) ? bd.asks.slice(0, 20) : [];
                if (!asks.length) return 0;
                var maxUsd = Math.max.apply(null, asks.map(function (x) { return Number(x.price) * Number(x.quantity); }));
                if (maxUsd <= 0) return 0;
                return Math.min(100, ((Number(a.price) * Number(a.quantity)) / maxUsd) * 100);
            },
            bookDepthSpreadPct: function (rowIndex) {
                var bd = this.selectedIteration && this.selectedIteration.book_depth;
                var bids = (bd && bd.bids) ? bd.bids.slice(0, 20) : [];
                var asks = (bd && bd.asks) ? bd.asks.slice(0, 20) : [];
                if (rowIndex >= bids.length || rowIndex >= asks.length) return '—';
                var bidPrice = Number(bids[rowIndex].price);
                var askPrice = Number(asks[rowIndex].price);
                if (!bidPrice || !askPrice || askPrice <= bidPrice) return '—';
                var mid = (bidPrice + askPrice) / 2;
                var spreadPct = (2 * (askPrice - bidPrice) / mid) * 100;
                return spreadPct.toFixed(2) + '%';
            },
            applyFilters: function () {
                this.page = 1;
                this.loadJobs();
            },
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
                var klines = this.selectedIteration && this.selectedIteration.klines;
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
                var klines = this.selectedIteration && this.selectedIteration.klines;
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
                var klines = this.selectedIteration && this.selectedIteration.klines;
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
                var klines = this.selectedIteration && this.selectedIteration.klines;
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
                var k = this.selectedIteration && this.selectedIteration.klines;
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
                var k = this.selectedIteration && this.selectedIteration.klines;
                return k && k.length ? k.length - 1 - displayIdx : 0;
            },
            onVolumeBarClick: function (c, originalIdx) {
                this.selectedVolumeCandleIndex = this.selectedVolumeCandleIndex === originalIdx ? null : originalIdx;
            }
        },
        computed: {
            klinesForChart: function () {
                var k = this.selectedIteration && this.selectedIteration.klines;
                if (!k || !k.length) return [];
                return k.slice().reverse();
            },
            klinesPriceTicks: function () {
                var k = this.selectedIteration && this.selectedIteration.klines;
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
                var k = this.selectedIteration && this.selectedIteration.klines;
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
            },
            totalPages: function () {
                return Math.max(1, Math.ceil(this.totalJobs / this.pageSize));
            },
            iterationsTotalPages: function () {
                return Math.max(1, Math.ceil(this.iterationsTotal / this.iterationsPageSize));
            },
            filteredJobIterations: function () {
                var q = (this.tokenSearchQuery || '').trim().toLowerCase();
                if (!q) return this.jobIterations;
                return this.jobIterations.filter(function (it) {
                    return (it.token || '').toLowerCase().indexOf(q) !== -1;
                });
            }
        },
        template:
            '<div>' +
            '<div class="card">' +
            '  <div class="card-header d-flex justify-content-between align-items-center">' +
            '    <h5 class="mb-0"><i class="bi bi-diagram-3 me-2"></i>Crawler: задания и итерации</h5>' +
            '    <button class="btn btn-sm btn-outline-primary" @click="loadStats(); loadJobs();"><i class="bi bi-arrow-clockwise"></i> Обновить</button>' +
            '  </div>' +
            '  <div class="card-body">' +
            '    <div v-if="error" class="alert alert-danger">[[ error ]]</div>' +
            '    <!-- Stats -->' +
            '    <div v-if="stats" class="row g-3 mb-4">' +
            '      <div class="col-md-2"><div class="card border-primary"><div class="card-body py-2 text-center"><div class="text-muted small">Заданий</div><strong>[[ stats.total_jobs ]]</strong></div></div></div>' +
            '      <div class="col-md-2"><div class="card border-info"><div class="card-body py-2 text-center"><div class="text-muted small">Итераций</div><strong>[[ stats.total_iterations ]]</strong></div></div></div>' +
            '      <div class="col-md-4">' +
            '        <div class="card border-secondary"><div class="card-body py-2">' +
            '          <div class="text-muted small">По биржам</div>' +
            '          <span v-for="x in (stats.by_exchange || [])" :key="x.exchange" class="badge bg-secondary me-1">[[ x.exchange ]] [[ x.count ]]</span>' +
            '        </div></div>' +
            '      </div>' +
            '      <div class="col-md-4">' +
            '        <div class="card border-secondary"><div class="card-body py-2">' +
            '          <div class="text-muted small">По статусу итераций</div>' +
            '          <span v-for="s in (stats.by_status || [])" :key="s.status" class="badge me-1" :class="s.status === \'success\' ? \'bg-success\' : s.status === \'error\' ? \'bg-danger\' : \'bg-warning text-dark\'">[[ s.status ]] [[ s.count ]]</span>' +
            '        </div></div>' +
            '      </div>' +
            '    </div>' +
            '    <!-- Filters -->' +
            '    <div class="row g-2 mb-3">' +
            '      <div class="col-auto"><select class="form-select form-select-sm" v-model="exchangeFilter" @change="applyFilters"><option value="">Все биржи</option><option v-for="e in exchanges" :key="e" :value="e">[[ e ]]</option></select></div>' +
            '      <div class="col-auto"><select class="form-select form-select-sm" v-model="connectorFilter" @change="applyFilters"><option value="">Spot/Perp</option><option value="spot">spot</option><option value="perpetual">perpetual</option></select></div>' +
            '      <div class="col-auto"><button class="btn btn-sm btn-primary" @click="applyFilters">Применить</button></div>' +
            '    </div>' +
            '    <!-- Jobs table -->' +
            '    <div v-if="loading" class="text-center py-4"><div class="spinner-border text-primary"></div></div>' +
            '    <div v-else>' +
            '      <div class="table-responsive">' +
            '        <table class="table table-hover table-sm">' +
            '          <thead><tr><th>ID</th><th>Биржа</th><th>Коннектор</th><th>Старт</th><th>Стоп</th><th>Итераций</th><th>Ошибка</th><th></th></tr></thead>' +
            '          <tbody>' +
            '            <tr v-for="j in jobs" :key="j.id" @click="selectJob(j)" style="cursor:pointer">' +
            '              <td>[[ j.id ]]</td><td>[[ j.exchange ]]</td><td>[[ j.connector ]]</td>' +
            '              <td>[[ formatDate(j.start) ]]</td><td>[[ formatDate(j.stop) ]]</td>' +
            '              <td>[[ j.iterations_count != null ? j.iterations_count : \'—\' ]]</td>' +
            '              <td><span v-if="j.error" class="text-danger small text-truncate d-inline-block" style="max-width:120px" :title="j.error">[[ j.error ]]</span><span v-else>—</span></td>' +
            '              <td><i class="bi bi-chevron-right"></i></td>' +
            '            </tr>' +
            '          </tbody>' +
            '        </table>' +
            '      </div>' +
            '      <nav v-if="totalPages > 1" class="mt-2"><ul class="pagination pagination-sm">' +
            '        <li class="page-item" :class="{disabled: page <= 1}"><a class="page-link" href="#" @click.prevent="page--; loadJobs()">Назад</a></li>' +
            '        <li class="page-item disabled"><span class="page-link">[[ page ]] / [[ totalPages ]]</span></li>' +
            '        <li class="page-item" :class="{disabled: page >= totalPages}"><a class="page-link" href="#" @click.prevent="page++; loadJobs()">Вперёд</a></li>' +
            '      </ul></nav>' +
            '    </div>' +
            '    <!-- Job detail panel (iterations) -->' +
            '    <div v-if="selectedJob" class="mt-4 border rounded p-3 bg-white">' +
            '      <div class="d-flex justify-content-between align-items-center mb-2">' +
            '        <h6 class="mb-0">Задание #[[ selectedJob.id ]] — [[ selectedJob.exchange ]] / [[ selectedJob.connector ]]</h6>' +
            '        <button class="btn btn-sm btn-outline-secondary" @click="closeJobPanel">Закрыть</button>' +
            '      </div>' +
            '      <div class="mb-2 d-flex flex-wrap align-items-center gap-2">' +
            '        <select class="form-select form-select-sm d-inline-block w-auto" v-model="statusFilter" @change="iterationsPage=1; loadIterations()"><option value="">Все статусы</option><option value="init">init</option><option value="pending">pending</option><option value="success">success</option><option value="error">error</option><option value="ignore">ignore</option></select>' +
            '        <input type="text" class="form-control form-control-sm d-inline-block" style="width: 140px" placeholder="Поиск по токену" v-model="tokenSearchQuery" />' +
            '        <span class="text-muted small">Всего: [[ iterationsTotal ]] [[ tokenSearchQuery.trim() ? \'(показано \' + filteredJobIterations.length + \')\' : \'\' ]]</span>' +
            '      </div>' +
            '      <div class="table-responsive"><table class="table table-sm">' +
            '        <thead><tr><th>ID</th><th>Токен</th><th>Старт</th><th>Стоп</th><th>Статус</th><th>done</th><th></th></tr></thead>' +
            '        <tbody>' +
            '          <tr v-for="it in filteredJobIterations" :key="it.id">' +
            '            <td>[[ it.id ]]</td><td>[[ it.token ]]</td>' +
            '            <td>[[ formatDate(it.start) ]]</td><td>[[ formatDate(it.stop) ]]</td>' +
            '            <td><span class="badge" :class="it.status === \'success\' ? \'bg-success\' : it.status === \'error\' ? \'bg-danger\' : \'bg-warning text-dark\'">[[ it.status ]]</span></td>' +
            '            <td>[[ it.done ? \'✓\' : \'—\' ]]</td>' +
            '            <td><button class="btn btn-sm btn-outline-primary" @click.stop="showIteration(it)">Детали</button></td>' +
            '          </tr>' +
            '        </tbody>' +
            '      </table></div>' +
            '      <nav v-if="iterationsTotalPages > 1" class="mt-2"><ul class="pagination pagination-sm">' +
            '        <li class="page-item" :class="{disabled: iterationsPage <= 1}"><a class="page-link" href="#" @click.prevent="iterationsPage--; loadIterations()">Назад</a></li>' +
            '        <li class="page-item disabled"><span class="page-link">[[ iterationsPage ]] / [[ iterationsTotalPages ]]</span></li>' +
            '        <li class="page-item" :class="{disabled: iterationsPage >= iterationsTotalPages}"><a class="page-link" href="#" @click.prevent="iterationsPage++; loadIterations()">Вперёд</a></li>' +
            '      </ul></nav>' +
            '    </div>' +
            '  </div>' +
            '</div>' +
            '<!-- Iteration detail modal (modal-window style) -->' +
            '<modal-window v-if="showIterationModal" width="90%" height="90%">' +
            '  <template slot="header">' +
            '    <div class="d-flex justify-content-between align-items-center w-100">' +
            '      <h6 class="mb-0">Итерация #[[ selectedIteration ? selectedIteration.id : \'\' ]] — [[ selectedIteration ? selectedIteration.token : \'\' ]]</h6>' +
            '      <button type="button" class="btn-close" @click="closeIterationModal"></button>' +
            '    </div>' +
            '  </template>' +
            '  <template slot="body">' +
            '    <div v-if="loadingIterationDetail" class="text-center py-5"><div class="spinner-border text-primary"></div><p class="mt-2 mb-0">Загрузка...</p></div>' +
            '    <div v-else-if="selectedIteration" class="iteration-detail-body">' +
            '      <p class="mb-2"><strong>Статус:</strong> [[ selectedIteration.status ]] <strong>done:</strong> [[ selectedIteration.done ]] &nbsp; <strong>Старт:</strong> [[ formatDate(selectedIteration.start) ]] <strong>Стоп:</strong> [[ formatDate(selectedIteration.stop) ]]</p>' +
            '      <p v-if="selectedIteration.error" class="mb-2"><strong>Ошибка:</strong> <span class="text-danger">[[ selectedIteration.error ]]</span></p>' +
            '      <!-- Order Book (Binance style) -->' +
            '      <div v-if="selectedIteration.book_depth" class="order-book-binance mb-4">' +
            '        <div class="order-book-header">' +
            '          <span class="order-book-title"><i class="bi bi-graph-up me-1"></i>Order Book</span>' +
            '          <span class="order-book-symbol">[[ selectedIteration.book_depth.symbol || selectedIteration.token || \'\' ]]</span>' +
            '        </div>' +
            '        <div class="order-book-columns">' +
            '          <div class="order-book-col order-book-bids">' +
            '            <table class="order-book-table"><thead><tr><th class="order-book-combo-th"><span class="ob-price-label">PRICE</span><span class="ob-usd-label">$</span></th></tr></thead><tbody>' +
            '              <tr v-for="(b, i) in (selectedIteration.book_depth.bids || []).slice(0, 20)" :key="\'b\'+i">' +
            '                <td class="order-book-combo-cell bid-price"><div class="depth-bar bid-bar" :style="{ width: depthPctBid(b) + \'%\' }"></div><span class="ob-price">[[ formatOrderPrice(b.price) ]]</span><span class="ob-usd">[[ formatUsd(Number(b.price) * Number(b.quantity)) ]]</span></td>' +
            '              </tr></tbody></table>' +
            '          </div>' +
            '          <div class="order-book-divider">' +
            '            <table class="order-book-table order-book-divider-table"><thead><tr><th>Δ%</th></tr></thead><tbody>' +
            '              <tr v-for="(n, i) in 20" :key="\'d\'+i"><td class="order-book-pct-cell">[[ bookDepthSpreadPct(i) ]]</td></tr>' +
            '            </tbody></table>' +
            '          </div>' +
            '          <div class="order-book-col order-book-asks order-book-asks-mirrored">' +
            '            <table class="order-book-table"><thead><tr><th class="order-book-combo-th"><span class="ob-price-label">PRICE</span><span class="ob-usd-label">$</span></th></tr></thead><tbody>' +
            '              <tr v-for="(a, i) in (selectedIteration.book_depth.asks || []).slice(0, 20)" :key="\'a\'+i">' +
            '                <td class="order-book-combo-cell ask-price"><div class="depth-bar ask-bar ask-bar-mirrored" :style="{ width: depthPctAsk(a) + \'%\' }"></div><span class="ob-price">[[ formatOrderPrice(a.price) ]]</span><span class="ob-usd">[[ formatUsd(Number(a.price) * Number(a.quantity)) ]]</span></td>' +
            '              </tr></tbody></table>' +
            '          </div>' +
            '        </div>' +
            '      </div>' +
            '      <!-- KLines (Binance style: шкала времени + шкала цен) -->' +
            '      <div v-if="selectedIteration.klines && selectedIteration.klines.length" class="klines-binance mb-4">' +
            '        <div class="klines-header">' +
            '          <span class="klines-title"><i class="bi bi-bar-chart-line me-1"></i>KLines</span>' +
            '          <span class="klines-symbol">[[ selectedIteration.token || selectedIteration.book_depth.symbol || \'\' ]]</span>' +
            '          <span v-if="selectedVolumeCandleIndex != null && selectedIteration.klines[selectedVolumeCandleIndex]" class="klines-volume-selected ms-2 text-nowrap" :title="getVolumeLabel(selectedIteration.klines[selectedVolumeCandleIndex])">[[ getVolumeLabel(selectedIteration.klines[selectedVolumeCandleIndex]) ]]</span>' +
            '        </div>' +
            '        <div class="klines-body">' +
            '          <div class="klines-row">' +
            '            <div class="klines-chart-wrap">' +
            '              <div class="klines-candles">' +
            '                <div v-for="(c, idx) in klinesForChart" :key="idx" class="candle-col" :style="candleColStyle(idx)" :title="formatKlineTooltip(c)">' +
            '                  <div class="candle-wick" :style="candleWickStyle(c)"></div>' +
            '                  <div class="candle-body" :class="c.close_price >= c.open_price ? \'candle-up\' : \'candle-down\'" :style="candleBodyStyle(c, idx)"></div>' +
            '                </div>' +
            '              </div>' +
            '            </div>' +
            '            <div class="klines-price-axis">' +
            '              <div v-for="(tick, i) in klinesPriceTicks" :key="\'p\'+i" class="klines-price-tick" :style="{ bottom: priceTickBottomPct(tick) + \'%\' }">[[ formatOrderPrice(tick) ]]</div>' +
            '            </div>' +
            '          </div>' +
            '          <div class="klines-volume-row">' +
            '            <div class="klines-volume-wrap">' +
            '              <div class="klines-volume-bars">' +
            '                <div v-for="(c, idx) in klinesForChart" :key="\'v\'+idx" class="volume-bar-col" :class="{ \'volume-bar-selected\': selectedVolumeCandleIndex === getOriginalKlineIndex(idx) }" :style="volumeBarStyle(c, idx)" @click.stop="onVolumeBarClick(c, getOriginalKlineIndex(idx))">' +
            '                  <div class="volume-bar" :class="c.close_price >= c.open_price ? \'volume-up\' : \'volume-down\'"></div>' +
            '                </div>' +
            '              </div>' +
            '            </div>' +
            '            <div class="klines-volume-axis">' +
            '              <div v-for="(tick, i) in klinesVolumeTicks" :key="\'v\'+i" class="klines-volume-tick" :style="{ bottom: volumeTickBottomPct(tick) + \'%\' }">[[ formatVolumeShort(tick) ]]</div>' +
            '            </div>' +
            '          </div>' +
            '          <div class="klines-time-row">' +
            '            <div class="klines-time-axis">' +
            '              <div v-for="(item, i) in klinesTimeLabels" :key="\'t\'+i" class="klines-time-tick" :style="{ left: item.leftPct + \'%\' }">[[ item.label ]]</div>' +
            '            </div>' +
            '            <div class="klines-time-axis-spacer"></div>' +
            '          </div>' +
            '        </div>' +
            '      </div>' +
            '      <div v-if="selectedIteration.funding_rate" class="mt-2"><strong>funding_rate:</strong> <pre class="bg-light p-2 rounded d-inline-block small mb-0" style="max-height:60px; overflow:auto">[[ JSON.stringify(selectedIteration.funding_rate, null, 2) ]]</pre></div>' +
            '    </div>' +
            '  </template>' +
            '  <template slot="footer">' +
            '    <button class="btn btn-secondary btn-sm" @click="closeIterationModal">Закрыть</button>' +
            '  </template>' +
            '</modal-window>' +
            '</div>'
    });

    return Vue;
}));
