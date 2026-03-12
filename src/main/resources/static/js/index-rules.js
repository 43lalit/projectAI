(function () {
  function createRulesManager(deps) {
    const el = deps.elements;
    let latestReportRules = [];

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
                  <div class="small text-muted">${deps.escapeHtml((row.ruleText || row.ruleExpression || '-').slice(0, 180))}</div>
                </td>
                ${reportCell(row)}
                <td>${deps.escapeHtml(row.scheduleName || '-')}</td>
                <td>${deps.escapeHtml(row.ruleType || '-')}</td>
                <td class="mono">${deps.escapeHtml(row.primaryMdrmCode || '-')}</td>
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
      const discrepancyOnly = !!el.reportRulesDiscrepancyOnly?.checked;
      const visibleRows = discrepancyOnly
        ? latestReportRules.filter(row => String(row.lineageStatus || '').toUpperCase() !== 'VALID')
        : latestReportRules;
      if (el.reportRulesMeta) {
        el.reportRulesMeta.textContent = meta || `Associated rules: ${deps.formatCount(visibleRows.length)}`;
      }
      if (el.reportRulesContainer) {
        el.reportRulesContainer.innerHTML = renderRuleRows(visibleRows, {
          includeReport: false,
          emptyMessage: discrepancyOnly ? 'No discrepancy rules for this report.' : 'No rules associated with this report.'
        });
      }
    }

    async function loadReportRules(reportingForm) {
      const form = String(reportingForm || '').trim();
      if (!form) {
        latestReportRules = [];
        if (el.reportRulesContainer) {
          el.reportRulesContainer.innerHTML = '<div class="text-muted small p-3">Select a report to inspect associated rules.</div>';
        }
        if (el.reportRulesMeta) {
          el.reportRulesMeta.textContent = 'Associated rules for the selected report.';
        }
        return;
      }
      if (el.reportRulesContainer) {
        el.reportRulesContainer.innerHTML = '<div class="text-muted small p-3">Loading report rules...</div>';
      }
      try {
        const response = await fetch(deps.appendRunContext(`/api/mdrm/rules/by-report?reportingForm=${encodeURIComponent(form)}`));
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
        const rows = Array.isArray(payload?.rules) ? payload.rules : [];
        renderReportRules(rows, `Associated rules: ${deps.formatCount(rows.length)} | discrepancies=${deps.formatCount(payload?.discrepancyCount || 0)} | as of ${payload?.asOfDate || 'latest'}`);
      } catch (error) {
        latestReportRules = [];
        if (el.reportRulesMeta) {
          el.reportRulesMeta.textContent = 'Unable to load report rules';
        }
        if (el.reportRulesContainer) {
          el.reportRulesContainer.innerHTML = `<div class="text-danger small p-3">Unable to load report rules: ${deps.escapeHtml(error.message || error)}</div>`;
        }
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
      refreshReportRulesFilter
    };
  }

  window.ProjectAiUiModules = window.ProjectAiUiModules || {};
  window.ProjectAiUiModules.createRulesManager = createRulesManager;
})();
