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
                loadingIterations: false,
                iterationsTotal: 0,
                iterationsPage: 1,
                iterationsPageSize: 50,
                statusFilter: '',
                tokenSearchQuery: '',
                hideIgnoreRecords: false,
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
                self.loadAllIterations();
            },
            loadAllIterations: function () {
                if (!this.selectedJob) return;
                var self = this;
                self.loadingIterations = true;
                var params = new URLSearchParams({ page: 1, page_size: 20000 });
                fetch('/api/admin/crawler/jobs/' + self.selectedJob.id + '/iterations?' + params)
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.jobIterations = data.iterations || [];
                        self.iterationsTotal = self.jobIterations.length;
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки итераций';
                    })
                    .finally(function () { self.loadingIterations = false; });
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
            resetIterationFilters: function () {
                this.statusFilter = '';
                this.tokenSearchQuery = '';
                this.hideIgnoreRecords = false;
                this.iterationsPage = 1;
            },
            onKlinesVolumeBarClick: function (originalIdx) {
                this.selectedVolumeCandleIndex = this.selectedVolumeCandleIndex === originalIdx ? null : originalIdx;
            }
        },
        computed: {
            totalPages: function () {
                return Math.max(1, Math.ceil(this.totalJobs / this.pageSize));
            },
            filteredJobIterations: function () {
                var list = this.jobIterations;
                var status = (this.statusFilter || '').trim();
                if (status) list = list.filter(function (it) { return it.status === status; });
                if (this.hideIgnoreRecords) list = list.filter(function (it) { return it.status !== 'ignore'; });
                var q = (this.tokenSearchQuery || '').trim().toLowerCase();
                if (q) list = list.filter(function (it) { return (it.token || '').toLowerCase().indexOf(q) !== -1; });
                return list;
            },
            iterationsTotalPages: function () {
                return Math.max(1, Math.ceil(this.filteredJobIterations.length / this.iterationsPageSize));
            },
            paginatedJobIterations: function () {
                var list = this.filteredJobIterations;
                var start = (this.iterationsPage - 1) * this.iterationsPageSize;
                return list.slice(start, start + this.iterationsPageSize);
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
            '          <span v-for="s in (stats.by_status || [])" :key="s.status" class="badge me-1" :class="s.status === \'success\' ? \'bg-success\' : s.status === \'error\' ? \'bg-danger\' : s.status === \'inactive\' ? \'bg-secondary\' : \'bg-warning text-dark\'">[[ s.status ]] [[ s.count ]]</span>' +
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
            '        <select class="form-select form-select-sm d-inline-block w-auto" v-model="statusFilter" @change="iterationsPage=1"><option value="">Все статусы</option><option value="init">init</option><option value="pending">pending</option><option value="success">success</option><option value="error">error</option><option value="ignore">ignore</option><option value="inactive">inactive</option></select>' +
            '        <input type="text" class="form-control form-control-sm d-inline-block" style="width: 140px" placeholder="Поиск по токену" v-model="tokenSearchQuery" @input="iterationsPage=1" />' +
            '        <label class="d-inline-flex align-items-center gap-1 mb-0"><input type="checkbox" v-model="hideIgnoreRecords" @change="iterationsPage=1" /> Игнорировать ignore записи</label>' +
            '        <span class="text-muted small">Всего: [[ filteredJobIterations.length ]] [[ tokenSearchQuery.trim() || statusFilter || hideIgnoreRecords ? \'(найдено \' + filteredJobIterations.length + \')\' : \'\' ]]</span>' +
            '        <button class="btn btn-sm btn-outline-secondary" @click="resetIterationFilters">Сбросить фильтры</button>' +
            '      </div>' +
            '      <div v-if="loadingIterations" class="text-center py-4"><div class="spinner-border text-primary"></div><p class="mt-2 mb-0 text-muted small">Загрузка итераций...</p></div>' +
            '      <template v-else>' +
            '      <div class="table-responsive"><table class="table table-sm">' +
            '        <thead><tr><th>ID</th><th>Токен</th><th>Старт</th><th>Стоп</th><th>Статус</th><th>done</th><th>Курс</th><th>FR</th><th>Next FR</th><th>Hist</th><th></th></tr></thead>' +
            '        <tbody>' +
            '          <tr v-for="it in paginatedJobIterations" :key="it.id">' +
            '            <td>[[ it.id ]]</td><td>[[ it.token ]]</td>' +
            '            <td>[[ formatDate(it.start) ]]</td><td>[[ formatDate(it.stop) ]]</td>' +
            '            <td><span class="badge" :class="it.status === \'success\' ? \'bg-success\' : it.status === \'error\' ? \'bg-danger\' : it.status === \'inactive\' ? \'bg-secondary\' : \'bg-warning text-dark\'">[[ it.status ]]</span></td>' +
            '            <td>[[ it.done ? \'✓\' : \'—\' ]]</td>' +
            '            <td>[[ it.currency_pair && it.currency_pair.ratio != null ? formatOrderPrice(it.currency_pair.ratio) : \'—\' ]]</td>' +
            '            <td><a href="#" v-if="it.funding_rate" @click.prevent="showFundingDetail(it, \'fr\')" class="small" :title="formatRatioPct(it.funding_rate.rate)">[[ formatRatioPct(it.funding_rate.rate) ]]</a><span v-else>—</span></td>' +
            '            <td><a href="#" v-if="it.next_funding_rate" @click.prevent="showFundingDetail(it, \'next\')" class="small">[[ it.next_funding_rate.next_funding_utc != null ? formatUtc(it.next_funding_rate.next_funding_utc) : (it.next_funding_rate.next_rate != null ? formatRatioPct(it.next_funding_rate.next_rate) : \'…\') ]]</a><span v-else>—</span></td>' +
            '            <td><a href="#" v-if="it.funding_rate_history && it.funding_rate_history.length" @click.prevent="showFundingDetail(it, \'hist\')" class="small">[[ it.funding_rate_history.length ]]</a><span v-else>—</span></td>' +
            '            <td><button class="btn btn-sm btn-outline-primary" @click.stop="showIteration(it)">Детали</button></td>' +
            '          </tr>' +
            '        </tbody>' +
            '      </table></div>' +
            '      <nav v-if="iterationsTotalPages > 1" class="mt-2"><ul class="pagination pagination-sm">' +
            '        <li class="page-item" :class="{disabled: iterationsPage <= 1}"><a class="page-link" href="#" @click.prevent="iterationsPage--">Назад</a></li>' +
            '        <li class="page-item disabled"><span class="page-link">[[ iterationsPage ]] / [[ iterationsTotalPages ]]</span></li>' +
            '        <li class="page-item" :class="{disabled: iterationsPage >= iterationsTotalPages}"><a class="page-link" href="#" @click.prevent="iterationsPage++">Вперёд</a></li>' +
            '      </ul></nav>' +
            '      </template>' +
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

    // --- Crawler2: вкладки по Job, статистика Job, таблица итераций как в Crawler ---
    Vue.component('crawler2-admin', {
        delimiters: ['[[', ']]'],
        data: function () {
            return {
                loading: true,
                error: null,
                jobs: [],
                selectedJob: null,
                jobStats: null,
                jobIterations: [],
                loadingIterations: false,
                iterationsPage: 1,
                iterationsPageSize: 50,
                statusFilter: '',
                tokenSearchQuery: '',
                hideIgnoreRecords: false,
                selectedIteration: null,
                showIterationModal: false,
                loadingIterationDetail: false,
                selectedVolumeCandleIndex: null,
                fundingDetailModal: { show: false, title: '', token: '', type: '', data: null }
            };
        },
        mounted: function () {
            this.loadJobs();
        },
        methods: {
            loadJobs: function () {
                var self = this;
                self.loading = true;
                self.error = null;
                fetch('/api/admin/crawler/jobs?page=1&page_size=100')
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.jobs = data.jobs || [];
                        if (self.jobs.length && !self.selectedJob) {
                            self.selectJob(self.jobs[0]);
                        } else if (self.selectedJob && !self.jobs.find(function (j) { return j.id === self.selectedJob.id; })) {
                            self.selectedJob = self.jobs[0] || null;
                            self.jobStats = null;
                            if (self.selectedJob) { self.loadJobStats(); self.loadAllIterations(); }
                        }
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки заданий';
                    })
                    .finally(function () { self.loading = false; });
            },
            selectJob: function (job) {
                this.selectedJob = job;
                this.jobStats = null;
                this.jobIterations = [];
                this.iterationsPage = 1;
                this.loadJobStats();
                this.loadAllIterations();
            },
            loadJobStats: function () {
                if (!this.selectedJob) return;
                var self = this;
                fetch('/api/admin/crawler/jobs/' + this.selectedJob.id + '/stats')
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.jobStats = data;
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки статистики job';
                    });
            },
            loadAllIterations: function () {
                if (!this.selectedJob) return;
                var self = this;
                self.loadingIterations = true;
                var params = new URLSearchParams({ page: 1, page_size: 20000 });
                fetch('/api/admin/crawler/jobs/' + this.selectedJob.id + '/iterations?' + params)
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.jobIterations = data.iterations || [];
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки итераций';
                    })
                    .finally(function () { self.loadingIterations = false; });
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
            formatRatioPct: function (ratio) {
                if (ratio == null) return '—';
                var n = Number(ratio);
                if (isNaN(n)) return '—';
                return (n * 100).toFixed(4) + '%';
            },
            formatDate: function (s) {
                if (!s) return '—';
                try {
                    var d = new Date(s);
                    return d.toLocaleString('ru-RU');
                } catch (e) { return s; }
            },
            closeIterationModal: function () {
                this.showIterationModal = false;
                this.selectedIteration = null;
                this.selectedVolumeCandleIndex = null;
            },
            formatPrice: function (p) {
                if (p == null || isNaN(p)) return '—';
                return Number(p).toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
            },
            resetIterationFilters: function () {
                this.statusFilter = '';
                this.tokenSearchQuery = '';
                this.hideIgnoreRecords = false;
                this.iterationsPage = 1;
            },
            onKlinesVolumeBarClick: function (originalIdx) {
                this.selectedVolumeCandleIndex = this.selectedVolumeCandleIndex === originalIdx ? null : originalIdx;
            }
        },
        computed: {
            filteredJobIterations: function () {
                var list = this.jobIterations;
                var status = (this.statusFilter || '').trim();
                if (status) list = list.filter(function (it) { return it.status === status; });
                if (this.hideIgnoreRecords) list = list.filter(function (it) { return it.status !== 'ignore'; });
                var q = (this.tokenSearchQuery || '').trim().toLowerCase();
                if (q) list = list.filter(function (it) {
                    return (it.token || '').toLowerCase().indexOf(q) !== -1 ||
                        (it.symbol || '').toLowerCase().indexOf(q) !== -1;
                });
                return list;
            },
            iterationsTotalPages: function () {
                return Math.max(1, Math.ceil(this.filteredJobIterations.length / this.iterationsPageSize));
            },
            paginatedJobIterations: function () {
                var list = this.filteredJobIterations;
                var start = (this.iterationsPage - 1) * this.iterationsPageSize;
                return list.slice(start, start + this.iterationsPageSize);
            }
        },
        template:
            '<div>' +
            '<div class="card">' +
            '  <div class="card-header d-flex justify-content-between align-items-center">' +
            '    <h5 class="mb-0"><i class="bi bi-collection me-2"></i>Crawler: задания (вкладки) и итерации</h5>' +
            '    <button class="btn btn-sm btn-outline-primary" @click="loadJobs"><i class="bi bi-arrow-clockwise"></i> Обновить</button>' +
            '  </div>' +
            '  <div class="card-body">' +
            '    <div v-if="error" class="alert alert-danger">[[ error ]]</div>' +
            '    <div v-if="loading" class="text-center py-4"><div class="spinner-border text-primary"></div></div>' +
            '    <template v-else>' +
            '      <ul class="nav nav-tabs mb-3">' +
            '        <li class="nav-item" v-for="j in jobs" :key="j.id">' +
            '          <a class="nav-link" href="#" :class="{ active: selectedJob && selectedJob.id === j.id }" @click.prevent="selectJob(j)">' +
            '            [[ j.exchange ]] [[ j.kind || j.connector ]] #[[ j.id ]]' +
            '          </a>' +
            '        </li>' +
            '      </ul>' +
            '      <div v-if="selectedJob && jobStats" class="mb-4">' +
            '        <div class="row g-2 mb-2">' +
            '          <div class="col-auto"><span class="text-muted">Старт:</span> [[ formatDate(jobStats.start) ]]</div>' +
            '          <div class="col-auto"><span class="text-muted">Стоп:</span> [[ formatDate(jobStats.stop) ]]</div>' +
            '          <div class="col-auto"><span class="text-muted">Итераций:</span> [[ jobStats.iterations_count != null ? jobStats.iterations_count : \'—\' ]]</div>' +
            '        </div>' +
            '        <div class="mb-2" v-if="jobStats.by_status && jobStats.by_status.length">' +
            '          <span class="text-muted me-2">По статусу:</span>' +
            '          <span v-for="s in jobStats.by_status" :key="s.status" class="badge me-1" :class="s.status === \'success\' ? \'bg-success\' : s.status === \'error\' ? \'bg-danger\' : s.status === \'inactive\' ? \'bg-secondary\' : \'bg-warning text-dark\'">[[ s.status ]] [[ s.count ]]</span>' +
            '        </div>' +
            '        <div v-if="jobStats.error" class="alert alert-danger py-2 mb-0">[[ jobStats.error ]]</div>' +
            '      </div>' +
            '      <div v-if="selectedJob" class="mt-3">' +
            '        <div class="d-flex flex-wrap align-items-center gap-2 mb-2">' +
            '          <select class="form-select form-select-sm d-inline-block w-auto" v-model="statusFilter" @change="iterationsPage=1"><option value="">Все статусы</option><option value="init">init</option><option value="pending">pending</option><option value="success">success</option><option value="error">error</option><option value="ignore">ignore</option><option value="inactive">inactive</option></select>' +
            '          <input type="text" class="form-control form-control-sm d-inline-block" style="width: 140px" placeholder="Поиск по токену/символу" v-model="tokenSearchQuery" @input="iterationsPage=1" />' +
            '          <label class="d-inline-flex align-items-center gap-1 mb-0"><input type="checkbox" v-model="hideIgnoreRecords" @change="iterationsPage=1" /> Игнорировать ignore записи</label>' +
            '          <span class="text-muted small">Всего: [[ filteredJobIterations.length ]] [[ tokenSearchQuery.trim() || statusFilter || hideIgnoreRecords ? \'(найдено \' + filteredJobIterations.length + \')\' : \'\' ]]</span>' +
            '          <button class="btn btn-sm btn-outline-secondary" @click="resetIterationFilters">Сбросить фильтры</button>' +
            '        </div>' +
            '        <div v-if="loadingIterations" class="text-center py-4"><div class="spinner-border text-primary"></div><p class="mt-2 mb-0 text-muted small">Загрузка итераций...</p></div>' +
            '        <template v-else>' +
            '        <div class="table-responsive"><table class="table table-sm">' +
            '          <thead><tr><th>ID</th><th>Токен</th><th>Символ</th><th>Старт</th><th>Стоп</th><th>Неактивна до</th><th>Статус</th><th>done</th><th>comment</th><th>Курс</th><th>FR</th><th>Next FR</th><th>Hist</th><th></th></tr></thead>' +
            '          <tbody>' +
            '            <tr v-for="it in paginatedJobIterations" :key="it.id">' +
            '              <td>[[ it.id ]]</td><td>[[ it.token ]]</td><td>[[ it.symbol || \'—\' ]]</td>' +
            '              <td>[[ formatDate(it.start) ]]</td><td>[[ formatDate(it.stop) ]]</td>' +
            '              <td>[[ it.inactive_till_timestamp ? formatDate(it.inactive_till_timestamp) : \'—\' ]]</td>' +
            '              <td><span class="badge" :class="it.status === \'success\' ? \'bg-success\' : it.status === \'error\' ? \'bg-danger\' : it.status === \'inactive\' ? \'bg-secondary\' : \'bg-warning text-dark\'">[[ it.status ]]</span></td>' +
            '              <td>[[ it.done ? \'✓\' : \'—\' ]]</td>' +
            '              <td><span class="small text-muted" :title="it.comment" style="max-width:120px; display:inline-block; overflow:hidden; text-overflow:ellipsis">[[ it.comment || \'—\' ]]</span></td>' +
            '              <td>[[ it.currency_pair && it.currency_pair.ratio != null ? formatPrice(it.currency_pair.ratio) : \'—\' ]]</td>' +
            '              <td><a href="#" v-if="it.funding_rate" @click.prevent="showFundingDetail(it, \'fr\')" class="small" :title="formatRatioPct(it.funding_rate.rate)">[[ formatRatioPct(it.funding_rate.rate) ]]</a><span v-else>—</span></td>' +
            '              <td><a href="#" v-if="it.next_funding_rate" @click.prevent="showFundingDetail(it, \'next\')" class="small">[[ it.next_funding_rate.next_funding_utc != null ? formatUtc(it.next_funding_rate.next_funding_utc) : (it.next_funding_rate.next_rate != null ? formatRatioPct(it.next_funding_rate.next_rate) : \'…\') ]]</a><span v-else>—</span></td>' +
            '              <td><a href="#" v-if="it.funding_rate_history && it.funding_rate_history.length" @click.prevent="showFundingDetail(it, \'hist\')" class="small">[[ it.funding_rate_history.length ]]</a><span v-else>—</span></td>' +
            '              <td><button class="btn btn-sm btn-outline-primary" @click.stop="showIteration(it)">Детали</button></td>' +
            '            </tr>' +
            '          </tbody>' +
            '        </table></div>' +
            '        <nav v-if="iterationsTotalPages > 1" class="mt-2"><ul class="pagination pagination-sm">' +
            '          <li class="page-item" :class="{disabled: iterationsPage <= 1}"><a class="page-link" href="#" @click.prevent="iterationsPage--">Назад</a></li>' +
            '          <li class="page-item disabled"><span class="page-link">[[ iterationsPage ]] / [[ iterationsTotalPages ]]</span></li>' +
            '          <li class="page-item" :class="{disabled: iterationsPage >= iterationsTotalPages}"><a class="page-link" href="#" @click.prevent="iterationsPage++">Вперёд</a></li>' +
            '        </ul></nav>' +
            '        </template>' +
            '      </div>' +
            '    </template>' +
            '  </div>' +
            '</div>' +
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

    // --- Tokens (admin) ---
    Vue.component('Tokens', {
        delimiters: ['[[', ']]'],
        data: function () {
            return {
                tokens: [],
                total: 0,
                page: 1,
                pageSize: 20,
                loading: true,
                error: null,
                filterSymbol: '',
                filterSource: '',
                showFormModal: false,
                formToken: null,
                formSymbol: '',
                saving: false,
                confirmDuplicate: { show: false, message: '', existingToken: null },
                pendingAction: null,
                confirmDelete: { show: false, token: null },
                togglingId: null,
                deletingId: null
            };
        },
        mounted: function () {
            this.loadTokens();
        },
        methods: {
            loadTokens: function () {
                var self = this;
                self.loading = true;
                self.error = null;
                var params = new URLSearchParams({
                    page: self.page,
                    page_size: self.pageSize
                });
                if (self.filterSymbol) params.set('symbol', self.filterSymbol);
                if (self.filterSource) params.set('source', self.filterSource);
                fetch('/api/admin/tokens?' + params, { credentials: 'include' })
                    .then(function (r) { return r.json(); })
                    .then(function (data) {
                        self.tokens = data.tokens || [];
                        self.total = data.total || 0;
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка загрузки токенов';
                    })
                    .finally(function () { self.loading = false; });
            },
            applyFilters: function () {
                this.page = 1;
                this.loadTokens();
            },
            openAdd: function () {
                this.formToken = null;
                this.formSymbol = '';
                this.showFormModal = true;
            },
            openEdit: function (token) {
                this.formToken = token;
                this.formSymbol = token.symbol;
                this.showFormModal = true;
            },
            closeForm: function () {
                this.showFormModal = false;
                this.formToken = null;
                this.formSymbol = '';
                this.confirmDuplicate = { show: false, message: '', existingToken: null };
            },
            saveToken: function () {
                var self = this;
                var symbol = (self.formSymbol || '').trim().toUpperCase();
                if (!symbol) {
                    self.error = 'Введите символ токена';
                    return;
                }

                if (self.formToken) {
                    self.doPut(symbol);
                    return;
                }

                var existing = self.tokens.find(function (t) {
                    return t.symbol.toUpperCase() === symbol && t.source === 'manual';
                });
                if (existing) {
                    self.confirmDuplicate = {
                        show: true,
                        message: 'Токен с символом «' + symbol + '» (manual) уже существует. Открыть для редактирования?',
                        existingToken: existing
                    };
                    self.pendingAction = 'edit_existing';
                    return;
                }
                self.doPost(symbol);
            },
            onConfirmDuplicate: function () {
                if (this.pendingAction === 'edit_existing' && this.confirmDuplicate.existingToken) {
                    this.closeForm();
                    this.openEdit(this.confirmDuplicate.existingToken);
                }
                this.confirmDuplicate = { show: false, message: '', existingToken: null };
                this.pendingAction = null;
            },
            onCancelDuplicate: function () {
                this.confirmDuplicate = { show: false, message: '', existingToken: null };
                this.pendingAction = null;
            },
            doPost: function (symbol) {
                var self = this;
                self.saving = true;
                self.error = null;
                fetch('/api/admin/tokens', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ symbol: symbol })
                })
                    .then(function (r) {
                        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || 'Ошибка создания'); });
                        return r.json();
                    })
                    .then(function () {
                        self.closeForm();
                        self.loadTokens();
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка создания токена';
                    })
                    .finally(function () { self.saving = false; });
            },
            doPut: function (symbol) {
                var self = this;
                if (!self.formToken || self.formToken.source !== 'manual') return;
                self.saving = true;
                self.error = null;
                fetch('/api/admin/tokens/' + self.formToken.id, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ symbol: symbol })
                })
                    .then(function (r) {
                        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || 'Ошибка обновления'); });
                        return r.json();
                    })
                    .then(function () {
                        self.closeForm();
                        self.loadTokens();
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка обновления токена';
                    })
                    .finally(function () { self.saving = false; });
            },
            formatDate: function (s) {
                if (!s) return '';
                var d = new Date(s);
                return d.toLocaleString('ru-RU');
            },
            confirmDeleteToken: function (token) {
                this.confirmDelete = { show: true, token: token };
            },
            cancelDelete: function () {
                this.confirmDelete = { show: false, token: null };
            },
            doDelete: function () {
                var self = this;
                var token = self.confirmDelete.token;
                if (!token) { self.cancelDelete(); return; }
                self.deletingId = token.id;
                fetch('/api/admin/tokens/' + token.id, { method: 'DELETE', credentials: 'include' })
                    .then(function (r) {
                        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || 'Ошибка удаления'); });
                        return r.json();
                    })
                    .then(function () {
                        self.cancelDelete();
                        self.loadTokens();
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка удаления токена';
                        self.cancelDelete();
                    })
                    .finally(function () { self.deletingId = null; });
            },
            toggleActive: function (token) {
                var self = this;
                if (token.source !== 'manual') return;
                self.togglingId = token.id;
                var next = !token.is_active;
                fetch('/api/admin/tokens/' + token.id, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ is_active: next })
                })
                    .then(function (r) {
                        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || 'Ошибка'); });
                        return r.json();
                    })
                    .then(function (updated) {
                        var i = self.tokens.findIndex(function (t) { return t.id === token.id; });
                        if (i !== -1) self.tokens.splice(i, 1, updated);
                    })
                    .catch(function (e) {
                        self.error = e.message || 'Ошибка изменения статуса';
                    })
                    .finally(function () { self.togglingId = null; });
            }
        },
        template:
            '<div class="container-fluid">' +
            '  <div class="alert alert-info mb-4" role="alert">' +
            '    <i class="bi bi-info-circle me-2"></i>' +
            '    <strong>Токены в работе.</strong> Здесь перечислены токены, взятые в работу для расчёта спредов. Добавляйте символы вручную или используйте загрузку из CoinMarketCap.' +
            '  </div>' +
            '  <div class="card">' +
            '    <div class="card-header d-flex justify-content-between align-items-center">' +
            '      <span><i class="bi bi-currency-bitcoin me-1"></i>Токены</span>' +
            '      <button class="btn btn-primary btn-sm" @click="openAdd"><i class="bi bi-plus me-1"></i>Добавить</button>' +
            '    </div>' +
            '    <div class="card-body">' +
            '      <div v-if="error" class="alert alert-danger">[[ error ]]</div>' +
            '      <div class="row g-2 mb-3">' +
            '        <div class="col-md-4"><input type="text" class="form-control form-control-sm" v-model="filterSymbol" placeholder="Символ" @keyup.enter="applyFilters"/></div>' +
            '        <div class="col-md-4">' +
            '          <select class="form-select form-select-sm" v-model="filterSource">' +
            '            <option value="">Все источники</option>' +
            '            <option value="coinmarketcap">coinmarketcap</option>' +
            '            <option value="manual">manual</option>' +
            '          </select>' +
            '        </div>' +
            '        <div class="col-md-2"><button class="btn btn-secondary btn-sm w-100" @click="applyFilters">Фильтр</button></div>' +
            '      </div>' +
            '      <div v-if="loading" class="text-muted small">Загрузка...</div>' +
            '      <div v-else class="table-responsive">' +
            '        <table class="table table-sm table-hover">' +
            '          <thead><tr><th>ID</th><th>Символ</th><th>Источник</th><th>IsActive</th><th>Создан</th><th>Обновлён</th><th></th></tr></thead>' +
            '          <tbody>' +
            '            <tr v-for="t in tokens" :key="t.id">' +
            '              <td>[[ t.id ]]</td>' +
            '              <td>[[ t.symbol ]]</td>' +
            '              <td>[[ t.source ]]</td>' +
            '              <td>' +
            '                <span v-if="t.is_active" class="badge bg-success">Да</span>' +
            '                <span v-else class="badge bg-secondary">Нет</span>' +
            '              </td>' +
            '              <td>[[ formatDate(t.created_at) ]]</td>' +
            '              <td>[[ formatDate(t.updated_at) ]]</td>' +
            '              <td>' +
            '                <template v-if="t.source === \'manual\'">' +
            '                  <button class="btn btn-outline-primary btn-sm me-1" @click="openEdit(t)" title="Редактировать"><i class="bi bi-pencil"></i></button>' +
            '                  <button class="btn btn-outline-warning btn-sm me-1" :disabled="togglingId === t.id" @click="toggleActive(t)" :title="t.is_active ? \'Сделать неактивным\' : \'Сделать активным\'">' +
            '                    <span v-if="togglingId === t.id" class="spinner-border spinner-border-sm"></span>' +
            '                    <i v-else :class="t.is_active ? \'bi bi-pause-circle\' : \'bi bi-play-circle\'"></i>' +
            '                  </button>' +
            '                  <button class="btn btn-outline-danger btn-sm" :disabled="deletingId === t.id" @click="confirmDeleteToken(t)" title="Удалить">' +
            '                    <span v-if="deletingId === t.id" class="spinner-border spinner-border-sm"></span>' +
            '                    <i v-else class="bi bi-trash"></i>' +
            '                  </button>' +
            '                </template>' +
            '                <span v-else class="text-muted small">—</span>' +
            '              </td>' +
            '            </tr>' +
            '          </tbody>' +
            '        </table>' +
            '      </div>' +
            '      <div v-if="total > pageSize" class="mt-2 small text-muted">Показано [[ tokens.length ]] из [[ total ]]</div>' +
            '    </div>' +
            '  </div>' +
            '  <!-- Form modal -->' +
            '  <div v-if="showFormModal" class="modal fade show" style="display: block; background-color: rgba(0,0,0,0.5);" tabindex="-1">' +
            '    <div class="modal-dialog modal-dialog-centered">' +
            '      <div class="modal-content">' +
            '        <div class="modal-header">' +
            '          <h5 class="modal-title">[[ formToken ? "Редактировать токен" : "Добавить токен" ]]</h5>' +
            '          <button type="button" class="btn-close" @click="closeForm" :disabled="saving"></button>' +
            '        </div>' +
            '        <div class="modal-body">' +
            '          <label class="form-label">Символ</label>' +
            '          <input type="text" class="form-control" v-model="formSymbol" placeholder="BTC" :disabled="saving"/>' +
            '        </div>' +
            '        <div class="modal-footer">' +
            '          <button class="btn btn-secondary" @click="closeForm" :disabled="saving">Отмена</button>' +
            '          <button class="btn btn-primary" @click="saveToken" :disabled="saving">[[ saving ? "Сохранение…" : "Сохранить" ]]</button>' +
            '        </div>' +
            '      </div>' +
            '    </div>' +
            '  </div>' +
            '  <confirm-dialog :show="confirmDuplicate.show" title="Токен уже существует" :message="confirmDuplicate.message" confirmText="Открыть для редактирования" cancelText="Отмена" type="warning" @confirm="onConfirmDuplicate" @cancel="onCancelDuplicate"></confirm-dialog>' +
            '  <confirm-dialog :show="confirmDelete.show" title="Удалить токен" :message="confirmDelete.token ? (\'Удалить токен \' + confirmDelete.token.symbol + \'?\') : \'\'" confirmText="Удалить" cancelText="Отмена" type="danger" :loading="deletingId !== null" @confirm="doDelete" @cancel="cancelDelete"></confirm-dialog>' +
            '</div>'
    });

    // Страница «Crawler» в панели: обёртка над crawler-admin
    Vue.component('Crawler', {
        delimiters: ['[[', ']]'],
        template: '<div class="container-fluid"><crawler-admin></crawler-admin></div>'
    });

    // Страница «Crawler2»: вкладки по Job, статистика, таблица итераций
    Vue.component('Crawler2', {
        delimiters: ['[[', ']]'],
        template: '<div class="container-fluid"><crawler2-admin></crawler2-admin></div>'
    });

    return Vue;
}));
