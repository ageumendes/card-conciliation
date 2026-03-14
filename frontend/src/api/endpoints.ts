export const endpoints = {
  interdata: {
    ping: '/admin/interdata/ping',
    files: '/admin/interdata/files',
    scan: '/admin/interdata/import/scan',
    upload: '/admin/interdata/import/upload',
    importDetails: '/admin/interdata/import/details',
    sales: '/admin/interdata/sales',
    approve: '/admin/interdata/sales/approve',
    reconcileRun: '/admin/interdata/reconciliation/run',
  },
  acquirerImport: {
    ping: '/admin/acquirer-import/ping',
    upload: '/admin/acquirer-import/upload',
    sales: '/admin/acquirer-import/sales',
    finance: '/admin/acquirer-import/finance',
  },
  cieloEdi: {
    ping: '/admin/cielo-edi/ping',
    files: '/admin/cielo-edi/files',
    scan: '/admin/cielo-edi/scan',
    sync: '/admin/cielo-edi/sync',
  },
  remoteEdi: {
    ping: '/admin/remote-edi/ping',
    pull: '/admin/remote-edi/pull',
  },
  maintenance: {
    clearSipag: '/admin/maintenance/clear-sipag',
    clearAll: '/admin/maintenance/clear-all',
  },
  reconciliation: {
    run: '/admin/reconciliation/run',
    list: '/admin/reconciliation/list',
    status: '/admin/reconciliation/status',
    manual: '/admin/reconciliation/manual',
    auditDuplicates: '/admin/reconciliation/audit/duplicates',
    details: (id: number) => `/admin/reconciliation/${id}/details`,
  },
};
