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
                connectors: ['spot', 'perpetual'],
                fundingDetailModal: { show: false, title: '', token: '', type: '', data: null }
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
            showFundingDetail: function (it, type) {
                var title = type === 'fr' ? 'Funding rate' : type === 'next' ? 'Next funding rate' : 'Funding rate history';
                var data = type === 'fr' ? it.funding_rate : type === 'next' ? it.next_funding_rate : (it.funding_rate_history || null);
                if (!data && type === 'hist') data = [];
                this.fundingDetailModal = { show: true, title: title, token: it.token || '', type: type, data: data };
            },
            closeFundingDetailModal: function () {
                this.fundingDetailModal = { show: false, title: '', token: '', type: '', data: null };
            },
            formatUtc: function (utc) {
                if (utc == null) return '—';
                var t = Number(utc);
                if (t < 1e12) t *= 1000;
                var d = new Date(t);
                if (isNaN(d.getTime())) return '—';
                return d.toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'medium' });
            },
            /** Decimal rate (e.g. 0.0001) -> multiply by 100 for % (0.01%). Used for funding_rate, next_funding_rate, funding_rate_history. */
            formatRatioPct: function (ratio) {
                if (ratio == null) return '—';
                var n = Number(ratio);
                if (isNaN(n)) return '—';
                return (n * 100).toFixed(4) + '%';
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
            applyFilters: function () {
                this.page = 1;
                this.loadJobs();
            },
            onKlinesVolumeBarClick: function (originalIdx) {
                this.selectedVolumeCandleIndex = this.selectedVolumeCandleIndex === originalIdx ? null : originalIdx;
            }
        },
        computed: {
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
            '        <thead><tr><th>ID</th><th>Токен</th><th>Старт</th><th>Стоп</th><th>Статус</th><th>done</th><th>FR</th><th>Next FR</th><th>Hist</th><th></th></tr></thead>' +
            '        <tbody>' +
            '          <tr v-for="it in filteredJobIterations" :key="it.id">' +
            '            <td>[[ it.id ]]</td><td>[[ it.token ]]</td>' +
            '            <td>[[ formatDate(it.start) ]]</td><td>[[ formatDate(it.stop) ]]</td>' +
            '            <td><span class="badge" :class="it.status === \'success\' ? \'bg-success\' : it.status === \'error\' ? \'bg-danger\' : \'bg-warning text-dark\'">[[ it.status ]]</span></td>' +
            '            <td>[[ it.done ? \'✓\' : \'—\' ]]</td>' +
            '            <td><a href="#" v-if="it.funding_rate" @click.prevent="showFundingDetail(it, \'fr\')" class="small" :title="formatRatioPct(it.funding_rate.rate)">[[ formatRatioPct(it.funding_rate.rate) ]]</a><span v-else>—</span></td>' +
            '            <td><a href="#" v-if="it.next_funding_rate" @click.prevent="showFundingDetail(it, \'next\')" class="small">[[ it.next_funding_rate.next_funding_utc != null ? formatUtc(it.next_funding_rate.next_funding_utc) : (it.next_funding_rate.next_rate != null ? formatRatioPct(it.next_funding_rate.next_rate) : \'…\') ]]</a><span v-else>—</span></td>' +
            '            <td><a href="#" v-if="it.funding_rate_history && it.funding_rate_history.length" @click.prevent="showFundingDetail(it, \'hist\')" class="small">[[ it.funding_rate_history.length ]]</a><span v-else>—</span></td>' +
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
            '      <book-depth :book-depth="selectedIteration.book_depth" :token="selectedIteration.token"></book-depth>' +
            '      <klines-chart :klines="selectedIteration.klines" :token="selectedIteration.token" :symbol="(selectedIteration.book_depth && selectedIteration.book_depth.symbol) || \'\'" :selected-volume-candle-index="selectedVolumeCandleIndex" @volume-bar-click="onKlinesVolumeBarClick"></klines-chart>' +
            '    </div>' +
            '  </template>' +
            '  <template slot="footer">' +
            '    <button class="btn btn-secondary btn-sm" @click="closeIterationModal">Закрыть</button>' +
            '  </template>' +
            '</modal-window>' +
            '<!-- Funding detail modal (separate, compact) -->' +
            '<modal-window v-if="fundingDetailModal.show" width="500px" height="auto">' +
            '  <template slot="header">' +
            '    <div class="d-flex justify-content-between align-items-center w-100">' +
            '      <h6 class="mb-0">[[ fundingDetailModal.title ]] — [[ fundingDetailModal.token ]]</h6>' +
            '      <button type="button" class="btn-close" @click="closeFundingDetailModal"></button>' +
            '    </div>' +
            '  </template>' +
            '  <template slot="body">' +
            '    <div v-if="fundingDetailModal.type === \'fr\' && fundingDetailModal.data" class="small">' +
            '      <p class="mb-1"><strong>Rate:</strong> [[ formatRatioPct(fundingDetailModal.data.rate) ]]</p>' +
            '      <p class="mb-1"><strong>Next funding (UTC):</strong> [[ formatUtc(fundingDetailModal.data.next_funding_utc) ]]</p>' +
            '      <p class="mb-1" v-if="fundingDetailModal.data.next_rate != null"><strong>Next rate:</strong> [[ formatRatioPct(fundingDetailModal.data.next_rate) ]]</p>' +
            '      <p class="mb-1" v-if="fundingDetailModal.data.index_price != null"><strong>Index price:</strong> [[ fundingDetailModal.data.index_price ]]</p>' +
            '      <p class="mb-0" v-if="fundingDetailModal.data.utc != null"><strong>UTC:</strong> [[ formatUtc(fundingDetailModal.data.utc) ]]</p>' +
            '    </div>' +
            '    <div v-else-if="fundingDetailModal.type === \'next\' && fundingDetailModal.data" class="small">' +
            '      <p class="mb-1"><strong>Next funding (UTC):</strong> [[ formatUtc(fundingDetailModal.data.next_funding_utc) ]]</p>' +
            '      <p class="mb-0" v-if="fundingDetailModal.data.next_rate != null"><strong>Next rate:</strong> [[ formatRatioPct(fundingDetailModal.data.next_rate) ]]</p>' +
            '    </div>' +
            '    <div v-else-if="fundingDetailModal.type === \'hist\' && fundingDetailModal.data && fundingDetailModal.data.length" class="small">' +
            '      <div class="table-responsive" style="max-height:280px; overflow:auto">' +
            '        <table class="table table-sm">' +
            '          <thead><tr><th>Время (UTC)</th><th>Rate %</th></tr></thead>' +
            '          <tbody>' +
            '            <tr v-for="(row, i) in fundingDetailModal.data" :key="i">' +
            '              <td>[[ formatUtc(row.funding_time_utc) ]]</td><td>[[ formatRatioPct(row.rate) ]]</td>' +
            '            </tr>' +
            '          </tbody>' +
            '        </table>' +
            '      </div>' +
            '    </div>' +
            '    <div v-else class="text-muted small">Нет данных</div>' +
            '  </template>' +
            '  <template slot="footer">' +
            '    <button class="btn btn-secondary btn-sm" @click="closeFundingDetailModal">Закрыть</button>' +
            '  </template>' +
            '</modal-window>' +
            '</div>'
    });

    // Страница «Crawler» в панели: обёртка над crawler-admin
    Vue.component('Crawler', {
        delimiters: ['[[', ']]'],
        template: '<div class="container-fluid"><crawler-admin></crawler-admin></div>'
    });

    return Vue;
}));
