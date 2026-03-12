(function () {
  function createReportingManager(deps) {
    const el = deps.elements;

    function getFormsLoaded() {
      return deps.getFormsLoaded();
    }

    function setFormsLoaded(value) {
      deps.setFormsLoaded(value);
    }

    function getAllForms() {
      return deps.getAllForms();
    }

    function setAllForms(value) {
      deps.setAllForms(value);
    }

    function getSelectedReportingForm() {
      return deps.getSelectedReportingForm();
    }

    function setSelectedReportingForm(value) {
      deps.setSelectedReportingForm(value);
    }

    function getReportStatusFilter() {
      return deps.getReportStatusFilter();
    }

    function setReportStatusFilter(value) {
      deps.setReportStatusFilter(value);
    }

    function getReportFormStatusByForm() {
      return deps.getReportFormStatusByForm();
    }

    function setReportFormStatusByForm(value) {
      deps.setReportFormStatusByForm(value);
    }

    function getSummaryGridApi() {
      return deps.getSummaryGridApi();
    }

    function setSummaryGridApi(value) {
      deps.setSummaryGridApi(value);
    }

    function getActiveMdrmGridApi() {
      return deps.getActiveMdrmGridApi();
    }

    function setActiveMdrmGridApi(value) {
      deps.setActiveMdrmGridApi(value);
    }

    function getNetChangeGridApi() {
      return deps.getNetChangeGridApi();
    }

    function setNetChangeGridApi(value) {
      deps.setNetChangeGridApi(value);
    }

    function getCurrentRunHistoryRows() {
      return deps.getCurrentRunHistoryRows();
    }

    function setCurrentRunHistoryRows(value) {
      deps.setCurrentRunHistoryRows(value);
    }

    function getIsReportDescriptionExpanded() {
      return deps.getIsReportDescriptionExpanded();
    }

    function setIsReportDescriptionExpanded(value) {
      deps.setIsReportDescriptionExpanded(value);
    }

    function getLatestRunIdForSelectedReport() {
      return deps.getLatestRunIdForSelectedReport();
    }

    function setLatestRunIdForSelectedReport(value) {
      deps.setLatestRunIdForSelectedReport(value);
    }

    function getLatestReportTablePayload() {
      return deps.getLatestReportTablePayload();
    }

    function setLatestReportTablePayload(value) {
      deps.setLatestReportTablePayload(value);
    }

    function applySummaryGridTheme(theme) {
      const summaryGrid = document.getElementById('summaryAgGrid');
      if (!summaryGrid) {
        return;
      }
      const dark = theme === 'dark';
      summaryGrid.classList.toggle('ag-theme-quartz-dark', dark);
      summaryGrid.classList.toggle('ag-theme-quartz', !dark);
    }

    function applyActiveMdrmGridTheme(theme) {
      if (!el.activeMdrmGrid) {
        return;
      }
      const dark = theme === 'dark';
      el.activeMdrmGrid.classList.toggle('ag-theme-quartz-dark', dark);
      el.activeMdrmGrid.classList.toggle('ag-theme-quartz', !dark);
    }

    function applyNetChangeGridTheme(theme) {
      if (!el.netChangeGrid) {
        return;
      }
      const dark = theme === 'dark';
      el.netChangeGrid.classList.toggle('ag-theme-quartz-dark', dark);
      el.netChangeGrid.classList.toggle('ag-theme-quartz', !dark);
    }

    function setReportingBusy(isBusy) {
      el.reportSearchInput.disabled = isBusy;
      el.leftNavMode.disabled = isBusy;
      el.reportListContainer.classList.toggle('busy', isBusy);
    }

    function statusCapsule(value) {
      const raw = String(value ?? '').trim();
      const normalized = raw.toLowerCase();
      if (normalized === 'y' || normalized === 'active') {
        return '<span class="status-capsule active">Active</span>';
      }
      if (normalized === 'n' || normalized === 'inactive') {
        return '<span class="status-capsule inactive">Inactive</span>';
      }
      return deps.escapeHtml(raw || '-');
    }

    function clearReportData() {
      if (getSummaryGridApi()) {
        getSummaryGridApi().destroy();
        setSummaryGridApi(null);
      }
      if (getActiveMdrmGridApi()) {
        getActiveMdrmGridApi().destroy();
        setActiveMdrmGridApi(null);
      }
      if (getNetChangeGridApi()) {
        getNetChangeGridApi().destroy();
        setNetChangeGridApi(null);
      }
      el.summaryTableContainer.innerHTML = '';
      el.tableContainer.innerHTML = '';
      setCurrentRunHistoryRows([]);
      if (el.compareRunASelect) {
        el.compareRunASelect.innerHTML = '';
      }
      if (el.compareRunBSelect) {
        el.compareRunBSelect.innerHTML = '';
      }
      if (el.compareSummaryContainer) {
        el.compareSummaryContainer.innerHTML = '';
      }
      if (el.reportKpiRuns) el.reportKpiRuns.textContent = '-';
      if (el.reportKpiLatestRun) el.reportKpiLatestRun.textContent = '-';
      if (el.reportKpiActive) el.reportKpiActive.textContent = '-';
      if (el.reportKpiNetChange) el.reportKpiNetChange.textContent = '-';
      setLatestRunIdForSelectedReport(null);
      setLatestReportTablePayload(null);
      if (el.reportDescriptionLabel) {
        el.reportDescriptionLabel.textContent = '';
        el.reportDescriptionLabel.classList.add('is-collapsed');
      }
      if (el.reportDescriptionToggleBtn) {
        el.reportDescriptionToggleBtn.style.display = 'none';
        el.reportDescriptionToggleBtn.textContent = 'Read more';
      }
      setIsReportDescriptionExpanded(false);
      if (el.reportFullNameLabel) {
        el.reportFullNameLabel.textContent = '';
      }
      if (el.reportSourceLink) {
        el.reportSourceLink.style.display = 'none';
        el.reportSourceLink.removeAttribute('href');
      }
      updateSelectedReportStatusPill(getSelectedReportingForm() || '');
      if (el.reportMetadataRefreshBtn) {
        el.reportMetadataRefreshBtn.disabled = !deps.isAdminUser() || !getSelectedReportingForm();
      }
      switchReportTab('');
      syncReportEmptyState();
      deps.syncCopyLinkButtons();
    }

    function syncReportEmptyState() {
      const hasSelection = !!(getSelectedReportingForm() && String(getSelectedReportingForm()).trim());
      if (el.reportingLayout) {
        el.reportingLayout.classList.toggle('reporting-layout-empty', !hasSelection);
      }
      if (el.reportMainPane) {
        el.reportMainPane.classList.toggle('report-main-empty', !hasSelection);
      }
      if (el.reportEmptyState) {
        el.reportEmptyState.style.display = hasSelection ? 'none' : '';
      }
      if (el.reportHeaderCard) {
        el.reportHeaderCard.style.display = hasSelection ? '' : 'none';
      }
      if (el.tabPanels && el.tabPanels.length) {
        el.tabPanels.forEach(panel => {
          panel.style.display = hasSelection ? '' : 'none';
        });
      }
    }

    function syncDescriptionToggleVisibility() {
      if (!el.reportDescriptionLabel || !el.reportDescriptionToggleBtn) {
        return;
      }
      const text = String(el.reportDescriptionLabel.textContent || '').trim();
      if (!text) {
        el.reportDescriptionLabel.classList.add('is-collapsed');
        el.reportDescriptionToggleBtn.style.display = 'none';
        el.reportDescriptionToggleBtn.textContent = 'Read more';
        setIsReportDescriptionExpanded(false);
        return;
      }
      if (getIsReportDescriptionExpanded()) {
        el.reportDescriptionToggleBtn.style.display = '';
        el.reportDescriptionToggleBtn.textContent = 'Read less';
        return;
      }
      const shouldToggle = el.reportDescriptionLabel.scrollHeight > el.reportDescriptionLabel.clientHeight + 2;
      el.reportDescriptionToggleBtn.style.display = shouldToggle ? '' : 'none';
      if (!shouldToggle) {
        el.reportDescriptionLabel.classList.add('is-collapsed');
        el.reportDescriptionToggleBtn.textContent = 'Read more';
        setIsReportDescriptionExpanded(false);
      }
    }

    function setReportDescriptionExpanded(expanded) {
      setIsReportDescriptionExpanded(!!expanded);
      if (el.reportDescriptionLabel) {
        el.reportDescriptionLabel.classList.toggle('is-collapsed', !getIsReportDescriptionExpanded());
      }
      if (el.reportDescriptionToggleBtn) {
        el.reportDescriptionToggleBtn.textContent = getIsReportDescriptionExpanded() ? 'Read less' : 'Read more';
      }
      syncDescriptionToggleVisibility();
    }

    function renderReportMetadata(metadata) {
      const fullName = String(metadata && metadata.fullName ? metadata.fullName : '').trim();
      const description = String(metadata && metadata.description ? metadata.description : '').trim();
      const sourceUrl = String(metadata && metadata.sourceUrl ? metadata.sourceUrl : '').trim();
      if (el.reportFullNameLabel) {
        el.reportFullNameLabel.textContent = fullName;
      }
      if (el.reportDescriptionLabel) {
        el.reportDescriptionLabel.textContent = description;
        el.reportDescriptionLabel.classList.add('is-collapsed');
      }
      if (el.reportDescriptionToggleBtn) {
        el.reportDescriptionToggleBtn.textContent = 'Read more';
      }
      setIsReportDescriptionExpanded(false);
      requestAnimationFrame(syncDescriptionToggleVisibility);
      if (el.reportSourceLink) {
        if (sourceUrl) {
          el.reportSourceLink.href = sourceUrl;
          el.reportSourceLink.style.display = '';
        } else {
          el.reportSourceLink.style.display = 'none';
          el.reportSourceLink.removeAttribute('href');
        }
      }
    }

    function renderReportList(filteredForms) {
      if (el.leftNavMode.value !== 'reports') {
        el.reportListContainer.innerHTML = '<div class="text-muted small p-2">MDRM view coming soon.</div>';
        return;
      }
      if (!filteredForms.length) {
        el.reportListContainer.innerHTML = '<div class="text-muted small p-2">No matching reports.</div>';
        return;
      }
      el.reportListContainer.innerHTML = filteredForms.map(form => `
        <button class="report-item ${form === getSelectedReportingForm() ? 'active' : ''}" type="button" data-report="${deps.escapeHtml(form)}">
          <span>${deps.escapeHtml(form)}</span>
        </button>
      `).join('');
    }

    function getReportStatusForForm(form) {
      const statusMap = getReportFormStatusByForm();
      const statusEntry = statusMap && statusMap[form] ? statusMap[form] : null;
      if (!statusEntry || !statusEntry.status) {
        return 'NO_DATA';
      }
      return String(statusEntry.status).toUpperCase();
    }

    function updateSelectedReportStatusPill(form) {
      if (!el.reportHeaderStatusPill) {
        return;
      }
      if (!form) {
        el.reportHeaderStatusPill.style.display = 'none';
        el.reportHeaderStatusPill.textContent = '';
        el.reportHeaderStatusPill.className = 'report-status-pill neutral';
        return;
      }
      const status = getReportStatusForForm(form);
      el.reportHeaderStatusPill.style.display = '';
      if (status === 'ACTIVE') {
        el.reportHeaderStatusPill.textContent = 'Active';
        el.reportHeaderStatusPill.className = 'report-status-pill active';
        return;
      }
      if (status === 'INACTIVE') {
        el.reportHeaderStatusPill.textContent = 'Inactive';
        el.reportHeaderStatusPill.className = 'report-status-pill inactive';
        return;
      }
      el.reportHeaderStatusPill.textContent = 'No Data';
      el.reportHeaderStatusPill.className = 'report-status-pill neutral';
    }

    function applyReportStatusFilterButtons() {
      const buttons = [el.reportFilterAllBtn, el.reportFilterActiveBtn, el.reportFilterInactiveBtn];
      buttons.forEach(button => {
        if (!button) {
          return;
        }
        button.classList.toggle('active', button.dataset.statusFilter === getReportStatusFilter());
      });
    }

    function filterAndRenderReportList() {
      if (el.leftNavMode.value !== 'reports') {
        renderReportList([]);
        return;
      }
      renderReportList(getFilteredReportForms());
    }

    function getFilteredReportForms() {
      const q = el.reportSearchInput.value.trim().toLowerCase();
      return getAllForms().filter(form => {
        if (q && !form.toLowerCase().includes(q)) {
          return false;
        }
        if (getReportStatusFilter() === 'ALL') {
          return true;
        }
        return getReportStatusForForm(form) === getReportStatusFilter();
      });
    }

    function openProfileMenuForAuth() {
      if (el.profileMenu && el.profileMenuBtn) {
        el.profileMenu.classList.add('open');
        el.profileMenuBtn.setAttribute('aria-expanded', 'true');
      }
    }

    function buildMdrmBookmarkPayload(row) {
      const subtitle = [
        String(row.description || '').trim(),
        getSelectedReportingForm() ? `Form ${getSelectedReportingForm()}` : ''
      ].filter(Boolean).join(' | ');
      return deps.buildBookmarkPayload(
        row.mdrmCode,
        row.mdrmCode,
        subtitle,
        deps.buildMdrmProfileUrl(row.mdrmCode),
        'mdrm'
      );
    }

    function createMdrmGridOptions() {
      return {
        defaultColDef: {
          sortable: true,
          filter: true,
          resizable: true,
          floatingFilter: false
        },
        pagination: true,
        paginationPageSize: 50,
        paginationPageSizeSelector: [50, 100, 200],
        suppressCellFocus: true,
        rowHeight: 30,
        headerHeight: 34,
        animateRows: true,
        onCellClicked: async params => {
          if (!params || !params.column) {
            return;
          }
          const columnId = params.column.getColId();
          if (columnId === 'mdrmCode') {
            const code = String(params.data && params.data.mdrmCode ? params.data.mdrmCode : '').trim();
            if (code) {
              await deps.recordRecentItem({
                type: 'mdrm',
                key: code,
                label: code,
                subtitle: getSelectedReportingForm() ? `Reporting form: ${getSelectedReportingForm()}` : '',
                url: deps.buildMdrmProfileUrl(code)
              });
              deps.openOntologyInsight('mdrm', code).catch(() => {});
            }
            return;
          }
          if (columnId !== 'bookmarkAction') {
            return;
          }
          const row = params.data || {};
          const code = String(row.mdrmCode || '').trim();
          if (!code) {
            return;
          }
          try {
            if (!deps.getCurrentUser()) {
              deps.setAuthStatus('Login required to save bookmarks.', 'err');
              openProfileMenuForAuth();
              return;
            }
            if (row.bookmarked) {
              await deps.removeBookmarkByTypeAndKey('mdrm', code);
              return;
            }
            deps.showSaveBookmarkModal(buildMdrmBookmarkPayload(row));
          } catch (_) {
            deps.setAuthStatus('Bookmark action failed. Please retry.', 'err');
          }
        },
        columnDefs: [
          {
            headerName: 'MDRM',
            field: 'mdrmCode',
            minWidth: 150,
            maxWidth: 240,
            cellRenderer: params => {
              const code = String(params.value || '').trim();
              return code ? `<button class="ontology-ref-link mono" type="button">${deps.escapeHtml(code)}</button>` : '-';
            }
          },
          {
            headerName: 'Description',
            field: 'description',
            flex: 1,
            minWidth: 340,
            valueFormatter: params => String(params.value || '-')
          },
          {
            headerName: 'Favorite',
            colId: 'bookmarkAction',
            field: 'bookmarked',
            minWidth: 92,
            maxWidth: 120,
            sortable: true,
            filter: false,
            resizable: false,
            cellStyle: { textAlign: 'center' },
            comparator: (valueA, valueB) => Number(Boolean(valueA)) - Number(Boolean(valueB)),
            cellRenderer: params => {
              const code = String(params.data && params.data.mdrmCode ? params.data.mdrmCode : '').trim();
              if (!code) {
                return '-';
              }
              const isBookmarked = !!(params.data && params.data.bookmarked);
              return `<button class="bookmark-star-btn bookmark-item-btn ${isBookmarked ? 'active' : ''}"
                        type="button"
                        title="${isBookmarked ? 'Bookmarked' : 'Save bookmark'}"
                        data-bookmarked="${isBookmarked ? 'true' : 'false'}">${isBookmarked ? '★' : '☆'}</button>`;
            }
          }
        ],
        rowData: []
      };
    }

    function initActiveMdrmGrid() {
      if (getActiveMdrmGridApi() || !el.activeMdrmGrid || !window.agGrid) {
        return;
      }
      setActiveMdrmGridApi(agGrid.createGrid(el.activeMdrmGrid, createMdrmGridOptions()));
      applyActiveMdrmGridTheme(document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light');
    }

    function initNetChangeGrid() {
      if (getNetChangeGridApi() || !el.netChangeGrid || !window.agGrid) {
        return;
      }
      setNetChangeGridApi(agGrid.createGrid(el.netChangeGrid, createMdrmGridOptions()));
      applyNetChangeGridTheme(document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light');
    }

    function readRowValueByKeys(row, keys) {
      const map = {};
      Object.keys(row || {}).forEach(key => {
        map[String(key).toLowerCase()] = row[key];
      });
      for (const key of keys) {
        const value = map[String(key).toLowerCase()];
        if (value !== null && value !== undefined && String(value).trim() !== '') {
          return String(value).trim();
        }
      }
      return '';
    }

    function buildActiveMdrmRows(payload) {
      if (!payload || !Array.isArray(payload.rows)) {
        return [];
      }
      const rows = payload.rows.map(row => {
        const activeFlag = String(readRowValueByKeys(row, ['is_active', 'status']) || '').toUpperCase();
        if (!(activeFlag === 'Y' || activeFlag === 'ACTIVE')) {
          return null;
        }
        const mdrmCode = readRowValueByKeys(row, ['mdrm_code', 'mdrm']);
        if (!mdrmCode) {
          return null;
        }
        return {
          mdrmCode,
          description: readRowValueByKeys(row, ['line_description', 'description', 'item_name', 'definition']),
          bookmarked: deps.isBookmarkedByTypeAndKey('mdrm', mdrmCode)
        };
      }).filter(Boolean);
      const dedup = new Map();
      rows.forEach(row => {
        if (!dedup.has(row.mdrmCode)) {
          dedup.set(row.mdrmCode, row);
        }
      });
      return Array.from(dedup.values());
    }

    function buildDescriptionByCodeMap(payload) {
      const map = new Map();
      if (!payload || !Array.isArray(payload.rows)) {
        return map;
      }
      payload.rows.forEach(row => {
        const code = readRowValueByKeys(row, ['mdrm_code', 'mdrm']);
        if (!code || map.has(code)) {
          return;
        }
        map.set(code, readRowValueByKeys(row, ['line_description', 'description', 'item_name', 'definition']));
      });
      return map;
    }

    function renderActiveMdrmPanel() {
      initActiveMdrmGrid();
      if (!getActiveMdrmGridApi()) {
        return;
      }
      const rows = buildActiveMdrmRows(getLatestReportTablePayload());
      getActiveMdrmGridApi().setGridOption('rowData', rows);
      if (el.activeMdrmQuickFilterInput) {
        el.activeMdrmQuickFilterInput.value = '';
      }
      getActiveMdrmGridApi().setGridOption('quickFilterText', '');
    }

    async function renderNetChangePanel() {
      initNetChangeGrid();
      if (!getNetChangeGridApi()) {
        return;
      }
      if (!getSelectedReportingForm() || !getLatestRunIdForSelectedReport()) {
        getNetChangeGridApi().setGridOption('rowData', []);
        if (el.netChangeInfoLabel) {
          el.netChangeInfoLabel.textContent = 'Modified MDRMs (latest run)';
        }
        return;
      }
      if (el.netChangeInfoLabel) {
        el.netChangeInfoLabel.textContent = `Loading modified MDRMs for run ${getLatestRunIdForSelectedReport()}...`;
      }
      const descriptionByCode = buildDescriptionByCodeMap(getLatestReportTablePayload());
      try {
        const query = encodeURIComponent(getSelectedReportingForm());
        const response = await fetch(`/api/mdrm/run-incremental-mdrms?reportingForm=${query}&runId=${encodeURIComponent(getLatestRunIdForSelectedReport())}&changeType=MODIFIED`);
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${JSON.stringify(payload)}`);
        }
        const rows = (Array.isArray(payload.mdrmCodes) ? payload.mdrmCodes : []).map(code => {
          const normalizedCode = String(code || '').trim();
          return {
            mdrmCode: normalizedCode,
            description: descriptionByCode.get(normalizedCode) || '',
            bookmarked: deps.isBookmarkedByTypeAndKey('mdrm', normalizedCode)
          };
        });
        getNetChangeGridApi().setGridOption('rowData', rows);
        if (el.netChangeQuickFilterInput) {
          el.netChangeQuickFilterInput.value = '';
        }
        getNetChangeGridApi().setGridOption('quickFilterText', '');
        if (el.netChangeInfoLabel) {
          el.netChangeInfoLabel.textContent = `Modified MDRMs for run ${getLatestRunIdForSelectedReport()}: ${rows.length}`;
        }
      } catch (_) {
        getNetChangeGridApi().setGridOption('rowData', []);
        if (el.netChangeInfoLabel) {
          el.netChangeInfoLabel.textContent = 'Unable to load modified MDRMs';
        }
      }
    }

    function renderTable(payload) {
      setLatestReportTablePayload(payload || null);
      if (!payload || !payload.columns || !payload.rows) {
        el.tableContainer.innerHTML = '';
        return;
      }
      const headerHtml = payload.columns.map(c => `<th>${deps.escapeHtml(c)}</th>`).join('');
      const rowsHtml = payload.rows.map(row => {
        const cells = payload.columns.map(c => {
          const lower = String(c || '').toLowerCase();
          const value = row[c];
          if (lower === 'is_active' || lower === 'status') {
            return `<td>${statusCapsule(value)}</td>`;
          }
          if (lower === 'mdrm_code') {
            const code = String(value || '').trim();
            return `<td>${code ? `<button class="ontology-ref-link mono" type="button" data-inline-insight-type="mdrm" data-inline-insight-value="${deps.escapeHtml(code)}">${deps.escapeHtml(code)}</button>` : '-'}</td>`;
          }
          if (lower === 'reporting_form') {
            const report = String(value || '').trim();
            return `<td>${report ? `<button class="ontology-ref-link" type="button" data-inline-insight-type="report" data-inline-insight-value="${deps.escapeHtml(report)}">${deps.escapeHtml(report)}</button>` : '-'}</td>`;
          }
          return `<td>${deps.escapeHtml(value)}</td>`;
        }).join('');
        return `<tr>${cells}</tr>`;
      }).join('');
      el.tableContainer.innerHTML = `
        <table class="table table-sm table-striped table-hover mb-0">
          <thead><tr>${headerHtml}</tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      `;
    }

    function setReportKpis(runs) {
      const list = Array.isArray(runs) ? runs : [];
      if (!list.length) {
        if (el.reportKpiRuns) el.reportKpiRuns.textContent = '0';
        if (el.reportKpiLatestRun) el.reportKpiLatestRun.textContent = '-';
        if (el.reportKpiActive) el.reportKpiActive.textContent = '0';
        if (el.reportKpiNetChange) el.reportKpiNetChange.textContent = '0';
        setLatestRunIdForSelectedReport(null);
        return;
      }
      const sorted = [...list].sort((a, b) => Number(b.runDatetime || 0) - Number(a.runDatetime || 0));
      const latest = sorted[0];
      setLatestRunIdForSelectedReport(Number(latest.runId || 0) || null);
      const latestActive = Number(latest.activeMdrms || 0);
      const net = Number(latest.modifiedMdrms || 0);
      if (el.reportKpiRuns) el.reportKpiRuns.textContent = deps.formatCount(list.length);
      if (el.reportKpiLatestRun) el.reportKpiLatestRun.textContent = deps.formatRunDate(latest.runDatetime) || '-';
      if (el.reportKpiActive) el.reportKpiActive.textContent = deps.formatCount(latestActive);
      if (el.reportKpiNetChange) {
        el.reportKpiNetChange.textContent = deps.formatCount(net);
      }
    }

    function renderCompareSelectors(runs) {
      if (!el.compareRunASelect || !el.compareRunBSelect) {
        return;
      }
      const list = Array.isArray(runs) ? runs : [];
      const options = list.map(run => {
        const label = `${run.runId} • ${deps.formatRunDate(run.runDatetime) || '-'} • ${run.fileName || '-'}`;
        return `<option value="${deps.escapeHtml(run.runId)}">${deps.escapeHtml(label)}</option>`;
      }).join('');
      el.compareRunASelect.innerHTML = options;
      el.compareRunBSelect.innerHTML = options;
      if (list.length) {
        el.compareRunASelect.value = String(list[Math.min(1, list.length - 1)].runId);
        el.compareRunBSelect.value = String(list[0].runId);
      }
      renderRunCompare();
    }

    function renderRunCompare() {
      if (!el.compareSummaryContainer) {
        return;
      }
      const runAId = Number(el.compareRunASelect?.value || 0);
      const runBId = Number(el.compareRunBSelect?.value || 0);
      const runA = getCurrentRunHistoryRows().find(run => Number(run.runId) === runAId);
      const runB = getCurrentRunHistoryRows().find(run => Number(run.runId) === runBId);
      if (!runA || !runB) {
        el.compareSummaryContainer.innerHTML = '<div class="text-muted small p-2">Select baseline and target runs.</div>';
        return;
      }
      const metrics = [
        { key: 'activeMdrms', label: 'Active' },
        { key: 'inactiveMdrms', label: 'Inactive' },
        { key: 'updatedMdrms', label: 'Updated' },
        { key: 'addedMdrms', label: 'Added' },
        { key: 'modifiedMdrms', label: 'Modified' },
        { key: 'deletedMdrms', label: 'Deleted' }
      ];
      const rows = metrics.map(metric => {
        const a = Number(runA[metric.key] || 0);
        const b = Number(runB[metric.key] || 0);
        const delta = b - a;
        const sign = delta > 0 ? '+' : '';
        return `<tr><td>${deps.escapeHtml(metric.label)}</td><td>${deps.formatCount(a)}</td><td>${deps.formatCount(b)}</td><td>${sign}${deps.formatCount(delta)}</td></tr>`;
      }).join('');
      el.compareSummaryContainer.innerHTML = `
        <table class="table table-sm table-striped table-hover mb-0">
          <thead>
            <tr><th>Metric</th><th>Baseline (${deps.escapeHtml(runA.runId)})</th><th>Target (${deps.escapeHtml(runB.runId)})</th><th>Delta</th></tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      `;
    }

    function renderRunHistory(payload) {
      const runs = payload && payload.runs ? payload.runs : [];
      setCurrentRunHistoryRows(runs);
      if (!runs.length) {
        if (getSummaryGridApi()) {
          getSummaryGridApi().destroy();
          setSummaryGridApi(null);
        }
        el.summaryTableContainer.innerHTML = '';
        setReportKpis([]);
        return;
      }
      setReportKpis(runs);
      renderCompareSelectors(runs);
      const rowData = runs.map(run => ({
        runId: Number(run.runId),
        fileName: String(run.fileName || ''),
        runDateDisplay: deps.formatRunDate(run.runDatetime),
        runMetaTooltip: `Run ID: ${run.runId} • Source File: ${run.fileName || '-'}`,
        totalUniqueMdrms: Number(run.totalUniqueMdrms || 0),
        activeMdrms: Number(run.activeMdrms || 0),
        inactiveMdrms: Number(run.inactiveMdrms || 0),
        updatedMdrms: Number(run.updatedMdrms || 0),
        addedMdrms: Number(run.addedMdrms || 0),
        modifiedMdrms: Number(run.modifiedMdrms || 0),
        deletedMdrms: Number(run.deletedMdrms || 0)
      }));

      if (!getSummaryGridApi()) {
        el.summaryTableContainer.innerHTML = '<div id="summaryAgGrid" class="mdrm-ag-grid run-timeline-grid"></div>';
        const summaryGrid = document.getElementById('summaryAgGrid');
        if (!summaryGrid || !window.agGrid) {
          return;
        }
        setSummaryGridApi(agGrid.createGrid(summaryGrid, {
          defaultColDef: {
            sortable: true,
            filter: true,
            resizable: true,
            floatingFilter: false,
            flex: 1,
            minWidth: 110
          },
          suppressCellFocus: true,
          rowHeight: 30,
          headerHeight: 34,
          animateRows: true,
          tooltipShowDelay: 150,
          tooltipMouseTrack: true,
          onFirstDataRendered: params => {
            if (params && params.api) {
              params.api.sizeColumnsToFit();
            }
          },
          onCellClicked: params => {
            if (!params || !params.column || !params.data) {
              return;
            }
            const bucketByColumn = {
              totalUniqueMdrms: 'TOTAL',
              activeMdrms: 'ACTIVE',
              inactiveMdrms: 'INACTIVE',
              updatedMdrms: 'UPDATED',
              addedMdrms: 'ADDED',
              modifiedMdrms: 'MODIFIED',
              deletedMdrms: 'DELETED'
            };
            const bucket = bucketByColumn[params.column.getColId()];
            if (!bucket || !getSelectedReportingForm()) {
              return;
            }
            if (params.event && typeof params.event.preventDefault === 'function') {
              params.event.preventDefault();
            }
            loadRunBucketDrilldown(params.data.runId, bucket);
          },
          columnDefs: [
            {
              headerName: 'Run Date',
              field: 'runDateDisplay',
              minWidth: 155,
              flex: 1.25,
              tooltipValueGetter: params => String(params.data?.runMetaTooltip || ''),
              cellRenderer: params => `<span title="${deps.escapeHtml(String(params.data?.runMetaTooltip || ''))}">${deps.escapeHtml(String(params.value || '-'))}</span>`
            },
            {
              headerName: 'Total Unique MDRMs',
              field: 'totalUniqueMdrms',
              minWidth: 135,
              cellRenderer: params => `<a class="count-link timeline-count-link" href="#">${deps.escapeHtml(deps.formatCount(params.value || 0))}</a>`
            },
            {
              headerName: 'Active',
              field: 'activeMdrms',
              minWidth: 95,
              cellRenderer: params => `<a class="count-link timeline-count-link" href="#">${deps.escapeHtml(deps.formatCount(params.value || 0))}</a>`
            },
            {
              headerName: 'Inactive',
              field: 'inactiveMdrms',
              minWidth: 95,
              cellRenderer: params => `<a class="count-link timeline-count-link" href="#">${deps.escapeHtml(deps.formatCount(params.value || 0))}</a>`
            },
            {
              headerName: 'Updated',
              field: 'updatedMdrms',
              minWidth: 95,
              cellRenderer: params => `<a class="count-link timeline-count-link" href="#">${deps.escapeHtml(deps.formatCount(params.value || 0))}</a>`
            },
            {
              headerName: 'Added',
              field: 'addedMdrms',
              minWidth: 90,
              cellRenderer: params => `<a class="count-link timeline-count-link" href="#">${deps.escapeHtml(deps.formatCount(params.value || 0))}</a>`
            },
            {
              headerName: 'Modified',
              field: 'modifiedMdrms',
              minWidth: 95,
              cellRenderer: params => `<a class="count-link timeline-count-link" href="#">${deps.escapeHtml(deps.formatCount(params.value || 0))}</a>`
            },
            {
              headerName: 'Deleted',
              field: 'deletedMdrms',
              minWidth: 90,
              cellRenderer: params => `<a class="count-link timeline-count-link" href="#">${deps.escapeHtml(deps.formatCount(params.value || 0))}</a>`
            }
          ],
          rowData: []
        }));
        applySummaryGridTheme(document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light');
        window.addEventListener('resize', () => {
          if (getSummaryGridApi()) {
            getSummaryGridApi().sizeColumnsToFit();
          }
        });
      }

      getSummaryGridApi().setGridOption('rowData', rowData);
    }

    async function loadRunBucketDrilldown(runId, bucket) {
      if (!getSelectedReportingForm()) {
        return;
      }
      switchReportTab('netChangeTab');
      initNetChangeGrid();
      if (!getNetChangeGridApi()) {
        return;
      }
      const query = encodeURIComponent(getSelectedReportingForm());
      if (el.netChangeInfoLabel) {
        el.netChangeInfoLabel.textContent = `Loading ${bucket} MDRMs for run ${runId}...`;
      }
      try {
        const normalizedBucket = String(bucket || '').toUpperCase();
        const isIncrementalBucket = normalizedBucket === 'ADDED' || normalizedBucket === 'MODIFIED' || normalizedBucket === 'DELETED';
        const url = isIncrementalBucket
          ? `/api/mdrm/run-incremental-mdrms?reportingForm=${query}&runId=${encodeURIComponent(runId)}&changeType=${encodeURIComponent(normalizedBucket)}`
          : `/api/mdrm/run-mdrms?reportingForm=${query}&runId=${encodeURIComponent(runId)}&bucket=${encodeURIComponent(normalizedBucket)}`;
        const response = await fetch(url);
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${JSON.stringify(payload)}`);
        }
        const codes = Array.isArray(payload.mdrmCodes) ? payload.mdrmCodes : [];
        const descriptionByCode = buildDescriptionByCodeMap(getLatestReportTablePayload());
        const rows = codes.map(code => {
          const normalizedCode = String(code || '').trim();
          return {
            mdrmCode: normalizedCode,
            description: descriptionByCode.get(normalizedCode) || ''
          };
        });
        getNetChangeGridApi().setGridOption('rowData', rows);
        if (el.netChangeQuickFilterInput) {
          el.netChangeQuickFilterInput.value = '';
        }
        getNetChangeGridApi().setGridOption('quickFilterText', '');
        if (el.netChangeInfoLabel) {
          el.netChangeInfoLabel.textContent = `Run ${runId} | ${normalizedBucket} MDRMs: ${rows.length}`;
        }
      } catch (_) {
        getNetChangeGridApi().setGridOption('rowData', []);
        if (el.netChangeInfoLabel) {
          el.netChangeInfoLabel.textContent = `Unable to load ${String(bucket || '').toUpperCase()} MDRMs`;
        }
      }
    }

    function switchReportTab(targetTabId) {
      const normalizedTarget = String(targetTabId || '').trim();
      el.tabPanels.forEach(panel => panel.classList.toggle('active', !!normalizedTarget && panel.id === normalizedTarget));
      el.tabButtons.forEach(button => {
        const active = button.dataset.tab === normalizedTarget;
        button.classList.toggle('active', active);
        button.setAttribute('aria-selected', String(active));
      });
    }

    async function refreshForms() {
      setReportingBusy(true);
      try {
        const [formsResponse, statusesResponse] = await Promise.all([
          fetch(deps.appendRunContext('/api/mdrm/reporting-forms')),
          fetch(deps.appendRunContext('/api/mdrm/reporting-form-statuses'))
        ]);
        const formsPayload = await formsResponse.json();
        const statusesPayload = await statusesResponse.json();
        if (!formsResponse.ok) {
          throw new Error(`HTTP ${formsResponse.status}: ${JSON.stringify(formsPayload)}`);
        }
        setAllForms(Array.isArray(formsPayload) ? formsPayload : []);
        if (getSelectedReportingForm() && !getAllForms().includes(getSelectedReportingForm())) {
          setSelectedReportingForm('');
          el.selectedReportLabel.textContent = '-';
          clearReportData();
        }
        setReportFormStatusByForm({});
        if (statusesResponse.ok && Array.isArray(statusesPayload)) {
          const nextStatusMap = {};
          statusesPayload.forEach(item => {
            const form = String(item.reportingForm || '').trim();
            if (form) {
              nextStatusMap[form] = item;
            }
          });
          setReportFormStatusByForm(nextStatusMap);
        }
        setFormsLoaded(true);
        if (el.kpiReportCount) {
          el.kpiReportCount.textContent = deps.formatCount(getAllForms().length);
        }
        const reportOption = el.leftNavMode.querySelector('option[value="reports"]');
        if (reportOption) {
          reportOption.textContent = `Reports (${getAllForms().length})`;
        }
        applyReportStatusFilterButtons();
        updateSelectedReportStatusPill(getSelectedReportingForm() || '');
        filterAndRenderReportList();
        deps.syncCopyLinkButtons();
      } catch (error) {
        console.error('Failed to load report list', error);
      } finally {
        setReportingBusy(false);
      }
    }

    async function loadTable(reportingForm) {
      if (!reportingForm) {
        return;
      }
      await deps.recordRecentItem({
        type: 'report',
        key: reportingForm,
        label: reportingForm,
        subtitle: 'Reporting form',
        url: '/index.html'
      });
      setReportingBusy(true);
      el.selectedReportLabel.textContent = reportingForm;
      updateSelectedReportStatusPill(reportingForm);
      deps.syncCopyLinkButtons();
      deps.updateReportBookmarkButton();
      deps.syncReportMetadataRefreshAccess();
      syncReportEmptyState();
      clearReportData();
      switchReportTab('timelineTab');
      if (el.reportDescriptionLabel) {
        el.reportDescriptionLabel.textContent = '';
      }
      if (el.reportFullNameLabel) {
        el.reportFullNameLabel.textContent = '';
      }
      if (el.reportSourceLink) {
        el.reportSourceLink.style.display = 'none';
        el.reportSourceLink.removeAttribute('href');
      }
      try {
        const query = encodeURIComponent(reportingForm);
        const [historyResponse, detailsResponse, metadataResponse] = await Promise.all([
          fetch(deps.appendRunContext(`/api/mdrm/run-history?reportingForm=${query}`)),
          fetch(deps.appendRunContext(`/api/mdrm/data?reportingForm=${query}`)),
          fetch(`/api/mdrm/report-metadata?reportingForm=${query}`)
        ]);
        const historyPayload = await historyResponse.json();
        const detailsPayload = await detailsResponse.json();
        const metadataPayload = await metadataResponse.json();
        if (!historyResponse.ok) {
          throw new Error(`Run history HTTP ${historyResponse.status}: ${JSON.stringify(historyPayload)}`);
        }
        if (!detailsResponse.ok) {
          throw new Error(`Details HTTP ${detailsResponse.status}: ${JSON.stringify(detailsPayload)}`);
        }
        if (metadataResponse.ok) {
          renderReportMetadata(metadataPayload);
        }
        renderRunHistory(historyPayload);
        renderTable(detailsPayload);
      } catch (error) {
        el.summaryTableContainer.innerHTML = '';
        el.tableContainer.innerHTML = '';
        console.error('Failed to load report data', error);
      } finally {
        setReportingBusy(false);
      }
    }

    async function toggleReportBookmark() {
      const reportingForm = String(getSelectedReportingForm() || '').trim();
      if (!reportingForm) {
        return;
      }
      if (!deps.getCurrentUser()) {
        deps.setAuthStatus('Login required to save bookmarks.', 'err');
        openProfileMenuForAuth();
        return;
      }
      if (deps.isSelectedReportBookmarked()) {
        await deps.removeBookmarkByTypeAndKey('report', reportingForm);
        return;
      }
      const fullName = String(el.reportFullNameLabel ? el.reportFullNameLabel.textContent : '').trim();
      const subtitle = fullName || 'Reporting form';
      deps.showSaveBookmarkModal(deps.buildBookmarkPayload(
        reportingForm,
        reportingForm,
        subtitle,
        deps.buildReportDeepLink(reportingForm),
        'report'
      ));
    }

    async function refreshSelectedReportMetadata() {
      if (!deps.isAdminUser() || !getSelectedReportingForm()) {
        return;
      }
      const query = encodeURIComponent(getSelectedReportingForm());
      if (el.reportMetadataRefreshBtn) {
        el.reportMetadataRefreshBtn.disabled = true;
      }
      try {
        const response = await deps.apiFetch(`/api/mdrm/report-metadata/refresh?reportingForm=${query}`, {
          method: 'POST'
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${JSON.stringify(payload)}`);
        }
        renderReportMetadata(payload);
      } catch (_) {
      } finally {
        deps.syncReportMetadataRefreshAccess();
      }
    }

    return {
      applySummaryGridTheme,
      applyActiveMdrmGridTheme,
      applyNetChangeGridTheme,
      setReportingBusy,
      clearReportData,
      syncReportEmptyState,
      syncDescriptionToggleVisibility,
      setReportDescriptionExpanded,
      renderReportMetadata,
      renderReportList,
      getReportStatusForForm,
      updateSelectedReportStatusPill,
      applyReportStatusFilterButtons,
      filterAndRenderReportList,
      getFilteredReportForms,
      initActiveMdrmGrid,
      initNetChangeGrid,
      readRowValueByKeys,
      buildActiveMdrmRows,
      buildDescriptionByCodeMap,
      renderActiveMdrmPanel,
      renderNetChangePanel,
      renderTable,
      setReportKpis,
      renderCompareSelectors,
      renderRunCompare,
      renderRunHistory,
      loadRunBucketDrilldown,
      switchReportTab,
      refreshForms,
      loadTable,
      toggleReportBookmark,
      refreshSelectedReportMetadata
    };
  }

  window.ProjectAiUiModules = Object.assign(window.ProjectAiUiModules || {}, {
    createReportingManager
  });
})();
