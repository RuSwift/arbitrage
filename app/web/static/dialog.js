/**
 * Confirm dialog Vue component (garantex-style).
 * Requires Vue to be loaded. Use delimiters [[ ]] to avoid Jinja2 conflict.
 */
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['vue'], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(require('vue'));
    } else {
        factory(root.Vue);
    }
}(typeof self !== 'undefined' ? self : this, function (Vue) {
    if (typeof Vue === 'undefined') return;

    Vue.component('confirm-dialog', {
        delimiters: ['[[', ']]'],
        props: {
            show: { type: Boolean, default: false },
            title: { type: String, default: 'Подтверждение' },
            message: { type: String, required: true },
            confirmText: { type: String, default: 'Подтвердить' },
            cancelText: { type: String, default: 'Отмена' },
            type: {
                type: String,
                default: 'warning',
                validator: function (value) {
                    return ['danger', 'warning', 'info', 'success'].indexOf(value) !== -1;
                }
            },
            width: { type: String, default: '500px' },
            showCancel: { type: Boolean, default: true },
            loading: { type: Boolean, default: false }
        },
        computed: {
            headerClass: function () {
                var classes = {
                    danger: 'bg-danger text-white',
                    warning: 'bg-warning text-dark',
                    info: 'bg-info text-white',
                    success: 'bg-success text-white'
                };
                return classes[this.type] || classes.warning;
            },
            confirmButtonClass: function () {
                var classes = {
                    danger: 'btn-danger',
                    warning: 'btn-warning',
                    info: 'btn-info',
                    success: 'btn-success'
                };
                return classes[this.type] || 'btn-warning';
            },
            icon: function () {
                var icons = {
                    danger: 'bi bi-exclamation-triangle',
                    warning: 'bi bi-exclamation-circle',
                    info: 'bi bi-info-circle',
                    success: 'bi bi-check-circle'
                };
                return icons[this.type] || icons.warning;
            }
        },
        methods: {
            handleConfirm: function () {
                this.$emit('confirm');
            },
            handleCancel: function () {
                this.$emit('cancel');
            },
            handleClose: function () {
                if (!this.loading) {
                    this.$emit('cancel');
                }
            }
        },
        template:
            '<div v-if="show" class="modal fade show" style="display: block; background-color: rgba(0, 0, 0, 0.5);" tabindex="-1" @click.self="handleClose">' +
            '  <div class="modal-dialog modal-dialog-centered">' +
            '    <div class="modal-content" style="box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15);">' +
            '      <div class="modal-header" :class="headerClass">' +
            '        <h5 class="modal-title"><i :class="icon + \' me-2\'"></i>[[ title ]]</h5>' +
            '        <button type="button" class="btn-close" :class="(type === \'danger\' || type === \'info\' || type === \'success\') ? \'btn-close-white\' : \'\'" @click="handleClose" :disabled="loading"></button>' +
            '      </div>' +
            '      <div class="modal-body" style="padding: 2rem;"><p class="mb-0">[[ message ]]</p></div>' +
            '      <div class="modal-footer">' +
            '        <button type="button" class="btn btn-secondary" @click="handleCancel" :disabled="loading" v-if="showCancel">[[ cancelText ]]</button>' +
            '        <button type="button" class="btn" :class="confirmButtonClass" @click="handleConfirm" :disabled="loading">' +
            '          <span v-if="loading" class="spinner-border spinner-border-sm me-2"></span>[[ confirmText ]]' +
            '        </button>' +
            '      </div>' +
            '    </div>' +
            '  </div>' +
            '</div>'
    });
}));
