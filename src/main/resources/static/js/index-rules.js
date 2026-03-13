(function () {
  function createRulesManager(deps) {
    const el = deps.elements;
    let latestReportRules = [];
    let reportRulesGridApi = null;
    let reportRulesGridHost = null;
    let reportRulesRequestId = 0;

    function renderExpandableBlock(value, options = {}) {
      const text = String(value ?? '').trim();
      const emptyValue = Object.prototype.hasOwnProperty.call(options, 'emptyValue') ? options.emptyValue : '-';
      const lineClamp = Math.max(1, Number(options.lineClamp || 2));
      const approxCharsPerLine = Math.max(30, Number(options.approxCharsPerLine || 72));
      if (!text) {
        return `<span>${deps.escapeHtml(emptyValue)}</span>`;
      }
      if (text.length <= lineClamp * approxCharsPerLine) {
        return `<span>${deps.escapeHtml(text)}</span>`;
      }
      return `
        <div class="expandable-block" data-expanded="false" style="--expandable-line-clamp:${lineClamp};">
          <div class="expandable-block-content">${deps.escapeHtml(text)}</div>
          <span class="expandable-block-toggle" role="button" tabindex="0" aria-expanded="false">Read more</span>
        </div>
      `;
    }

    function renderGridClampText(value, options = {}) {
      return `
        <div class="grid-clamp-text">
          ${renderExpandableBlock(value, options)}
        </div>
      `;
    }

    function renderMdrmLink(value) {
      const code = String(value || '').trim().toUpperCase();
      if (!code || code === '-') {
        return '<span>-</span>';
      }
      return `<button class="ontology-ref-link mono" type="button" data-mdrm-insight="${deps.escapeHtml(code)}">${deps.escapeHtml(code)}</button>`;
    }

    function applyReportRulesGridTheme(theme) {
      const host = document.getElementById('reportRulesAgGrid');
      if (!host) {
        return;
      }
      const dark = theme === 'dark';
      host.classList.toggle('ag-theme-quartz-dark', dark);
      host.classList.toggle('ag-theme-quartz', !dark);
    }

    function destroyReportRulesGrid() {
      if (reportRulesGridApi && typeof reportRulesGridApi.destroy === 'function') {
        reportRulesGridApi.destroy();
      }
      reportRulesGridApi = null;
      reportRulesGridHost = null;
    }

    function refreshReportRulesGridHeights() {
      if (reportRulesGridApi && typeof reportRulesGridApi.onRowHeightChanged === 'function') {
        reportRulesGridApi.onRowHeightChanged();
      }
    }

    function ensureReportRulesGrid() {
      if (!el.reportRulesContainer || !window.agGrid) {
        return null;
      }
      let host = document.getElementById('reportRulesAgGrid');
      if (!host) {
        el.reportRulesContainer.innerHTML = '<div id="reportRulesAgGrid" class="mdrm-ag-grid report-rules-grid ag-theme-quartz"></div>';
        host = document.getElementById('reportRulesAgGrid');
      }
      if (!host) {
        return null;
      }
      if (reportRulesGridApi && (!reportRulesGridHost || reportRulesGridHost !== host || !reportRulesGridHost.isConnected)) {
        destroyReportRulesGrid();
      }
      if (reportRulesGridApi) {
        applyReportRulesGridTheme(document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light');
        return reportRulesGridApi;
      }
      applyReportRulesGridTheme(document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light');
      reportRulesGridApi = agGrid.createGrid(host, {
        defaultColDef: {
          sortable: true,
          filter: true,
          resizable: true,
          floatingFilter: false,
          minWidth: 110,
          flex: 1,
          tooltipValueGetter: params => {
            const value = params?.value;
            return value == null ? '' : String(value);
          }
        },
        suppressCellFocus: true,
        rowSelection: {
          mode: 'singleRow',
          checkboxes: false,
          headerCheckbox: false,
          enableClickSelection: false
        },
        rowHeight: 32,
        headerHeight: 34,
        animateRows: true,
        tooltipShowDelay: 150,
        tooltipMouseTrack: true,
        overlayNoRowsTemplate: '<span class="ag-overlay-loading-center">No rules associated with this report.</span>',
        columnDefs: [
          {
            headerName: 'Rule',
            field: 'ruleNumber',
            minWidth: 120,
            maxWidth: 150,
            pinned: 'left'
          },
          {
            headerName: 'Description',
            field: 'ruleText',
            minWidth: 240,
            flex: 1.4,
            wrapText: true,
            autoHeight: true,
            cellClass: 'ag-cell-clamped-text',
            cellRenderer: params => renderGridClampText(params.value || '-', { lineClamp: 2, approxCharsPerLine: 72 })
          },
          {
            headerName: 'Alg Edit Test',
            field: 'ruleExpression',
            minWidth: 280,
            flex: 1.6,
            wrapText: true,
            autoHeight: true,
            cellClass: 'ag-cell-clamped-text',
            cellRenderer: params => renderGridClampText(params.value || '-', { lineClamp: 2, approxCharsPerLine: 80 })
          },
          {
            headerName: 'Schedule',
            field: 'scheduleName',
            minWidth: 120
          },
          {
            headerName: 'Type',
            field: 'ruleType',
            minWidth: 110
          },
          {
            headerName: 'Primary MDRM',
            field: 'primaryMdrmCode',
            minWidth: 130,
            cellRenderer: params => renderMdrmLink(params.value)
          },
          {
            headerName: 'Dependencies',
            field: 'dependencyCount',
            minWidth: 110,
            maxWidth: 130
          },
          {
            headerName: 'Lineage Status',
            field: 'lineageStatus',
            minWidth: 140,
            cellRenderer: params => statusBadge(params.value || '-')
          },
          {
            headerName: 'Effective Start',
            field: 'effectiveStartDate',
            minWidth: 120
          },
          {
            headerName: 'Effective End',
            field: 'effectiveEndDate',
            minWidth: 120
          }
        ]
      });
      reportRulesGridHost = host;
      return reportRulesGridApi;
    }

    function statusBadge(status) {
      const raw = String(status || 'VALID').trim();
      const normalized = raw.toLowerCase();
      const css = normalized === 'valid'
        ? 'active'
        : normalized.includes('inactive') || normalized.includes('missing')
          ? 'inactive'
          : 'neutral';
      return `<span class="status-capsule ${css}">${deps.escapeHtml(raw.replaceAll('_', ' '))}</span>`;
    }

    function setRulesLoadStatus(text, css) {
      if (!el.rulesLoadStatus) return;
      el.rulesLoadStatus.textContent = text;
      el.rulesLoadStatus.className = `status ${css}`;
    }

    function renderRuleRows(rows, { includeReport = true, emptyMessage = 'No rules found.' } = {}) {
      const list = Array.isArray(rows) ? rows : [];
      if (!list.length) {
        return `<div class="text-muted small p-3">${deps.escapeHtml(emptyMessage)}</div>`;
      }
      const reportHeader = includeReport ? '<th>Report</th>' : '';
      const reportCell = includeReport
        ? row => `<td>${deps.escapeHtml(row.reportingForm || row.reportSeries || '-')}</td>`
        : () => '';
      return `
        <table class="table table-sm table-striped table-hover mb-0">
          <thead>
            <tr>
              <th>Rule</th>
              ${reportHeader}
              <th>Schedule</th>
              <th>Type</th>
              <th>Primary MDRM</th>
              <th>Dependencies</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            ${list.map(row => `
              <tr>
                <td>
                  <div class="fw-semibold">${deps.escapeHtml(row.ruleNumber || String(row.ruleId || '-'))}</div>
                  <div class="small text-muted">${deps.renderExpandableText(row.ruleText || row.ruleExpression || '-', { maxLength: 100 })}</div>
                </td>
                ${reportCell(row)}
                <td>${deps.escapeHtml(row.scheduleName || '-')}</td>
                <td>${deps.escapeHtml(row.ruleType || '-')}</td>
                <td>${renderMdrmLink(row.primaryMdrmCode || '-')}</td>
                <td>${deps.escapeHtml(String(row.dependencyCount ?? 0))}</td>
                <td>${statusBadge(row.lineageStatus)}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      `;
    }

    async function refreshRulesLoadHistory() {
      if (!el.rulesLoadHistoryContainer) return;
      try {
        const response = await fetch('/api/mdrm/rules/load-history');
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
        const rows = Array.isArray(payload) ? payload : [];
        if (!rows.length) {
          el.rulesLoadHistoryContainer.innerHTML = '<div class="text-muted small p-3">No rule loads recorded yet.</div>';
          return;
        }
        el.rulesLoadHistoryContainer.innerHTML = `
          <table class="table table-sm table-striped table-hover mb-0">
            <thead>
              <tr>
                <th>Load ID</th>
                <th>Loaded At</th>
                <th>Source</th>
                <th>Series</th>
                <th>Rules</th>
                <th>Dependencies</th>
                <th>Warnings</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              ${rows.map(row => `
                <tr>
                  <td>${deps.escapeHtml(row.loadId)}</td>
                  <td>${deps.escapeHtml(deps.formatRunDate(row.loadedAt))}</td>
                  <td>${deps.escapeHtml(row.sourceFileName || '-')}</td>
                  <td>${deps.escapeHtml(row.reportSeries || '-')}</td>
                  <td>${deps.escapeHtml(row.totalRuleRows)}</td>
                  <td>${deps.escapeHtml(row.totalDependencyRows)}</td>
                  <td>${deps.escapeHtml(row.warningCount)}</td>
                  <td>${statusBadge(row.loadStatus)}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `;
      } catch (error) {
        el.rulesLoadHistoryContainer.innerHTML = `<div class="text-danger small p-3">Unable to load rule history: ${deps.escapeHtml(error.message || error)}</div>`;
      }
    }

    async function loadRulesWorkbook() {
      if (!deps.isAdminUser()) {
        setRulesLoadStatus('Admin access required to load the rules workbook.', 'err');
        return;
      }
      setRulesLoadStatus('Loading rules workbook...', 'neutral');
      try {
        const response = await deps.apiFetch('/api/mdrm/rules/load', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
        setRulesLoadStatus(
          `${payload.skippedDuplicate ? 'Workbook already loaded' : 'Rules workbook loaded'} | rules=${payload.loadedRules || 0} | dependencies=${payload.loadedDependencies || 0} | warnings=${payload.warningCount || 0}`,
          payload.skippedDuplicate ? 'neutral' : 'ok'
        );
        await refreshRulesLoadHistory();
      } catch (error) {
        setRulesLoadStatus(`Rules load failed: ${error.message || error}`, 'err');
      }
    }

    async function runDiscoveryRuleSearch(query) {
      const cleaned = String(query || '').trim();
      if (!el.discoveryRuleResults || !el.discoveryRuleTable || !el.discoveryRuleMeta) {
        return;
      }
      if (!cleaned) {
        el.discoveryRuleResults.style.display = 'none';
        el.discoveryRuleTable.innerHTML = '';
        el.discoveryRuleMeta.textContent = '';
        return;
      }
      try {
        const response = await fetch(deps.appendRunContext(`/api/mdrm/rules/search?q=${encodeURIComponent(cleaned)}`));
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
        const rows = Array.isArray(payload?.rules) ? payload.rules : [];
        el.discoveryRuleMeta.textContent = `Rule matches: ${deps.formatCount(rows.length)} | as of ${payload?.asOfDate || 'latest'}`;
        el.discoveryRuleTable.innerHTML = renderRuleRows(rows.slice(0, 80), {
          includeReport: true,
          emptyMessage: 'No rules matched this search.'
        });
        el.discoveryRuleResults.style.display = '';
      } catch (error) {
        el.discoveryRuleMeta.textContent = 'Rule search failed';
        el.discoveryRuleTable.innerHTML = `<div class="text-danger small p-3">Unable to load rules: ${deps.escapeHtml(error.message || error)}</div>`;
        el.discoveryRuleResults.style.display = '';
      }
    }

    function renderReportRules(rows, meta) {
      latestReportRules = Array.isArray(rows) ? rows : [];
      if (el.reportRulesMeta) {
        el.reportRulesMeta.textContent = meta || `Associated rules: ${deps.formatCount(latestReportRules.length)}`;
      }
      if (el.reportRulesContainer) {
        const gridApi = ensureReportRulesGrid();
        if (!gridApi) {
          el.reportRulesContainer.innerHTML = `<div class="text-danger small p-3">Rule grid is unavailable.</div>`;
          return;
        }
        gridApi.setGridOption('rowData', latestReportRules.map(row => ({
          ruleId: row.ruleId,
          ruleNumber: String(row.ruleNumber || row.ruleId || '-'),
          ruleText: String(row.ruleText || '-'),
          ruleExpression: String(row.ruleExpression || '-'),
          scheduleName: String(row.scheduleName || '-'),
          ruleType: String(row.ruleType || '-'),
          primaryMdrmCode: String(row.primaryMdrmCode || '-'),
          dependencyCount: Number(row.dependencyCount || 0),
          lineageStatus: String(row.lineageStatus || '-'),
          effectiveStartDate: String(row.effectiveStartDate || '-'),
          effectiveEndDate: String(row.effectiveEndDate || '-')
        })));
        if (latestReportRules.length) {
          gridApi.hideOverlay();
        } else {
          gridApi.showNoRowsOverlay();
        }
      }
    }

    async function loadReportRules(reportingForm) {
      const form = String(reportingForm || '').trim();
      const requestId = ++reportRulesRequestId;
      if (!form) {
        latestReportRules = [];
        destroyReportRulesGrid();
        if (el.reportKpiRules) {
          el.reportKpiRules.textContent = '0';
        }
        if (el.reportRulesContainer) {
          el.reportRulesContainer.innerHTML = '<div class="text-muted small p-3">Select a report to inspect associated rules.</div>';
        }
        if (el.reportRulesMeta) {
          el.reportRulesMeta.textContent = 'Associated rules for the selected report.';
        }
        return { reportingForm: '', totalRules: 0 };
      }
      if (el.reportRulesContainer && !reportRulesGridApi) {
        el.reportRulesContainer.innerHTML = '<div class="text-muted small p-3">Loading report rules...</div>';
      }
      if (el.reportKpiRules) {
        el.reportKpiRules.textContent = '...';
      }
      try {
        const response = await fetch(deps.appendRunContext(`/api/mdrm/rules/by-report?reportingForm=${encodeURIComponent(form)}`));
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
        if (requestId !== reportRulesRequestId || String(deps.getSelectedReportingForm?.() || '').trim() !== form) {
          return;
        }
        const rows = Array.isArray(payload?.rules) ? payload.rules : [];
        if (el.reportKpiRules) {
          el.reportKpiRules.textContent = deps.formatCount(Number(payload?.totalRules || rows.length));
        }
        renderReportRules(rows, `Associated rules: ${deps.formatCount(rows.length)} | discrepancies=${deps.formatCount(payload?.discrepancyCount || 0)} | as of ${payload?.asOfDate || 'latest'}`);
        return {
          reportingForm: form,
          totalRules: Number(payload?.totalRules || rows.length)
        };
      } catch (error) {
        if (requestId !== reportRulesRequestId || String(deps.getSelectedReportingForm?.() || '').trim() !== form) {
          return { reportingForm: form, totalRules: 0 };
        }
        latestReportRules = [];
        destroyReportRulesGrid();
        if (el.reportKpiRules) {
          el.reportKpiRules.textContent = '0';
        }
        if (el.reportRulesMeta) {
          el.reportRulesMeta.textContent = 'Unable to load report rules';
        }
        if (el.reportRulesContainer) {
          el.reportRulesContainer.innerHTML = `<div class="text-danger small p-3">Unable to load report rules: ${deps.escapeHtml(error.message || error)}</div>`;
        }
        return {
          reportingForm: form,
          totalRules: 0
        };
      }
    }

    function refreshReportRulesFilter() {
      renderReportRules(latestReportRules, null);
    }

    return {
      refreshRulesLoadHistory,
      loadRulesWorkbook,
      runDiscoveryRuleSearch,
      loadReportRules,
      refreshReportRulesFilter,
      applyReportRulesGridTheme,
      refreshReportRulesGridHeights
    };
  }

  window.ProjectAiUiModules = window.ProjectAiUiModules || {};
  window.ProjectAiUiModules.createRulesManager = createRulesManager;
})();
