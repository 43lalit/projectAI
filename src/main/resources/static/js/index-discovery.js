(function () {
  function createDiscoveryManager(deps) {
    const el = deps.elements;

    function getRecentItems() {
      return deps.getRecentItems();
    }

    function setRecentItems(value) {
      deps.setRecentItems(value);
    }

    function getRecentSearches() {
      return deps.getRecentSearches();
    }

    function setRecentSearches(value) {
      deps.setRecentSearches(value);
    }

    function getSavedSearches() {
      return deps.getSavedSearches();
    }

    function setSavedSearches(value) {
      deps.setSavedSearches(value);
    }

    function getLastDiscoveryRows() {
      return deps.getLastDiscoveryRows();
    }

    function setLastDiscoveryRows(value) {
      deps.setLastDiscoveryRows(value);
    }

    function getDiscoveryGridApi() {
      return deps.getDiscoveryGridApi();
    }

    function setDiscoveryGridApi(value) {
      deps.setDiscoveryGridApi(value);
    }

    function setDiscoveryMeta(text) {
      if (el.discoveryResultMeta) {
        el.discoveryResultMeta.textContent = text;
      }
    }

    function setDiscoveryResultsVisible(visible) {
      if (el.discoverySearchResults) {
        el.discoverySearchResults.style.display = visible ? '' : 'none';
      }
    }

    function formatCount(value) {
      const n = Number(value);
      if (Number.isNaN(n)) {
        return '0';
      }
      return n.toLocaleString('en-US');
    }

    function updateRecentPanelVisibility(inDiscoveryView) {
      if (!el.discoveryRecentPanel) {
        return;
      }
      const isDiscoveryActive = inDiscoveryView !== null && inDiscoveryView !== undefined
        ? inDiscoveryView
        : !!document.querySelector('#view-discovery.view.active');
      const hasRecentItems = Array.isArray(getRecentItems()) && getRecentItems().length > 0;
      el.discoveryRecentPanel.style.display = isDiscoveryActive && hasRecentItems ? '' : 'none';
    }

    function loadRecentItemsFromLocal() {
      const storage = deps.getRecentStorageForCurrentSession();
      try {
        const primaryKey = deps.getRecentItemsStorageKey();
        const parsedPrimary = JSON.parse(storage.getItem(primaryKey) || '[]');
        setRecentItems(Array.isArray(parsedPrimary) ? parsedPrimary : []);
        const runId = deps.getRunContextId();
        if (runId && (!getRecentItems() || getRecentItems().length === 0)) {
          const legacyKey = deps.localRecentItemsKey + '::latest';
          const parsedLegacy = JSON.parse(storage.getItem(legacyKey) || '[]');
          const legacyItems = Array.isArray(parsedLegacy) ? parsedLegacy : [];
          if (legacyItems.length) {
            setRecentItems(legacyItems.slice(0, deps.recentItemsMax));
            storage.setItem(primaryKey, JSON.stringify(getRecentItems()));
          }
        }
      } catch (_) {
        setRecentItems([]);
      }
      setRecentItems(
        (getRecentItems() || [])
          .filter(item => item && item.type && item.key && item.label)
          .slice(0, deps.recentItemsMax)
      );
    }

    async function loadRecentItems() {
      if (!deps.getCurrentUser()) {
        loadRecentItemsFromLocal();
        renderRecentItems();
        return;
      }
      try {
        const response = await fetch(deps.appendRunContext('/api/recent/items?limit=' + deps.recentItemsMax));
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        const payload = await response.json();
        setRecentItems((Array.isArray(payload) ? payload : []).map(item => ({
          type: String(item.itemType || '').trim().toLowerCase(),
          key: String(item.itemKey || '').trim(),
          label: String(item.label || item.itemKey || '').trim(),
          subtitle: String(item.subtitle || '').trim(),
          url: String(item.url || '').trim(),
          timestamp: Number(item.lastAccessed || Date.now())
        })));
      } catch (_) {
        loadRecentItemsFromLocal();
      }
      renderRecentItems();
    }

    function saveRecentItems() {
      const storage = deps.getRecentStorageForCurrentSession();
      storage.setItem(
        deps.getRecentItemsStorageKey(),
        JSON.stringify((getRecentItems() || []).slice(0, deps.recentItemsMax))
      );
    }

    function getSearchMemoryStorage() {
      try {
        return window.localStorage;
      } catch (_) {
        return window.localStorage;
      }
    }

    function loadSearchMemory() {
      const storage = getSearchMemoryStorage();
      try {
        const parsedRecent = JSON.parse(storage.getItem(deps.recentSearchesKey) || '[]');
        setRecentSearches(Array.isArray(parsedRecent) ? parsedRecent : []);
      } catch (_) {
        setRecentSearches([]);
      }
      try {
        const parsedSaved = JSON.parse(storage.getItem(deps.savedSearchesKey) || '[]');
        setSavedSearches(Array.isArray(parsedSaved) ? parsedSaved : []);
      } catch (_) {
        setSavedSearches([]);
      }
      setRecentSearches(
        (getRecentSearches() || [])
          .filter(item => item && String(item.query || '').trim())
          .slice(0, deps.recentSearchesMax)
      );
      setSavedSearches(
        (getSavedSearches() || [])
          .filter(item => item && item.state && String(item.name || '').trim())
          .slice(0, deps.savedSearchesMax)
      );
    }

    function persistSearchMemory() {
      const storage = getSearchMemoryStorage();
      storage.setItem(deps.recentSearchesKey, JSON.stringify((getRecentSearches() || []).slice(0, deps.recentSearchesMax)));
      storage.setItem(deps.savedSearchesKey, JSON.stringify((getSavedSearches() || []).slice(0, deps.savedSearchesMax)));
    }

    function normalizeSearchQuery(query) {
      return String(query || '').trim().toLowerCase();
    }

    function upsertRecentSearch(query) {
      const cleaned = String(query || '').trim();
      if (!cleaned) {
        return;
      }
      const normalized = normalizeSearchQuery(cleaned);
      setRecentSearches((getRecentSearches() || []).filter(item => normalizeSearchQuery(item.query) !== normalized));
      getRecentSearches().unshift({ query: cleaned, ts: Date.now() });
      setRecentSearches((getRecentSearches() || []).slice(0, deps.recentSearchesMax));
      persistSearchMemory();
      renderSearchMemory();
    }

    function renderRecentSearches() {
      if (!el.recentSearchesContainer) {
        return;
      }
      if (!(getRecentSearches() || []).length) {
        el.recentSearchesContainer.innerHTML = '<div class="text-muted small px-1">No recent searches yet.</div>';
        return;
      }
      el.recentSearchesContainer.innerHTML = (getRecentSearches() || []).map((item, index) => (
        '<button class="search-chip" type="button" data-recent-search-index="' + index + '" title="' + deps.escapeHtml(item.query) + '">' +
          deps.escapeHtml(item.query) +
        '</button>'
      )).join('');
    }

    function renderSavedSearches() {
      if (!el.savedSearchesContainer) {
        return;
      }
      if (!(getSavedSearches() || []).length) {
        el.savedSearchesContainer.innerHTML = '<div class="text-muted small px-1">No saved searches yet.</div>';
        return;
      }
      el.savedSearchesContainer.innerHTML = (getSavedSearches() || []).map((item, index) => (
        '<div class="search-chip-wrap">' +
          '<button class="search-chip saved" type="button" data-saved-search-index="' + index + '" title="' + deps.escapeHtml(item.name) + '">' + deps.escapeHtml(item.name) + '</button>' +
          '<button class="search-chip-remove" type="button" data-remove-saved-search-index="' + index + '" aria-label="Remove saved search">×</button>' +
        '</div>'
      )).join('');
    }

    function renderSearchMemory() {
      renderRecentSearches();
      renderSavedSearches();
      if (el.clearRecentSearchesBtn) {
        el.clearRecentSearchesBtn.disabled = !(getRecentSearches() || []).length;
      }
    }

    async function applySearchState(state) {
      if (!state) {
        return;
      }
      const targetRunId = Number(state.runId || 0);
      const currentRunId = Number(deps.getRunContextId() || 0);
      if (
        targetRunId > 0 &&
        el.runContextSelect &&
        targetRunId !== currentRunId &&
        Array.from(el.runContextSelect.options || []).some(option => Number(option.value) === targetRunId)
      ) {
        el.runContextSelect.value = String(targetRunId);
        await deps.applyRunContextChange();
      }
      deps.switchView('view-discovery');
      applyDiscoverySearchState(state);
      const desiredQuickFilter = String(state.qf || '').trim();
      const query = String(state.q || '').trim();
      if (!query) {
        setDiscoveryResultsVisible(false);
        deps.syncCopyLinkButtons();
        return;
      }
      await runDiscoverySearch();
      if (el.discoveryQuickFilterInput) {
        el.discoveryQuickFilterInput.value = desiredQuickFilter;
      }
      if (getDiscoveryGridApi()) {
        getDiscoveryGridApi().setGridOption('quickFilterText', desiredQuickFilter);
        updateDiscoveryMetaCounts();
      }
    }

    async function applyRecentSearchByIndex(index) {
      const entry = (getRecentSearches() || [])[index];
      if (!entry) {
        return;
      }
      await applySearchState({
        q: String(entry.query || '').trim(),
        fc: '',
        fd: '',
        fr: '',
        qf: '',
        runId: deps.getRunContextId()
      });
    }

    async function applySavedSearchByIndex(index) {
      const entry = (getSavedSearches() || [])[index];
      if (!entry || !entry.state) {
        return;
      }
      await applySearchState(entry.state);
    }

    function clearRecentSearches() {
      setRecentSearches([]);
      persistSearchMemory();
      renderSearchMemory();
    }

    function removeSavedSearchByIndex(index) {
      if (!Number.isInteger(index) || index < 0 || index >= (getSavedSearches() || []).length) {
        return;
      }
      const next = [...getSavedSearches()];
      next.splice(index, 1);
      setSavedSearches(next);
      persistSearchMemory();
      renderSearchMemory();
    }

    function saveCurrentSearch() {
      const state = getDiscoverySearchState();
      if (!state.q && !state.fc && !state.fd && !state.fr && !state.qf) {
        return;
      }
      const defaultName = state.q || state.fr || state.fc || 'Saved search';
      const nameInput = window.prompt('Name this saved search:', defaultName);
      const name = String(nameInput || '').trim();
      if (!name) {
        return;
      }
      const normalizedName = name.toLowerCase();
      const nextEntry = { name, state, ts: Date.now() };
      const nextSaved = (getSavedSearches() || []).filter(item => String(item.name || '').trim().toLowerCase() !== normalizedName);
      nextSaved.unshift(nextEntry);
      setSavedSearches(nextSaved.slice(0, deps.savedSearchesMax));
      persistSearchMemory();
      renderSearchMemory();
    }

    function showCopyFlash(target, message) {
      const anchor = target && target.getBoundingClientRect ? target : null;
      if (!anchor) {
        return;
      }
      const rect = anchor.getBoundingClientRect();
      const flash = document.createElement('div');
      flash.className = 'copy-flash';
      flash.textContent = message || 'Link copied';
      flash.style.left = Math.round(rect.left + rect.width / 2) + 'px';
      flash.style.top = Math.round(rect.top - 8) + 'px';
      document.body.appendChild(flash);
      window.requestAnimationFrame(() => flash.classList.add('show'));
      window.setTimeout(() => {
        flash.classList.remove('show');
        flash.addEventListener('transitionend', () => flash.remove(), { once: true });
      }, 780);
    }

    async function copyText(value, successMessage, triggerElement) {
      const text = String(value || '').trim();
      if (!text) {
        return;
      }
      try {
        if (navigator && navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
          await navigator.clipboard.writeText(text);
        } else {
          throw new Error('Clipboard API unavailable');
        }
        if (triggerElement) {
          showCopyFlash(triggerElement, successMessage || 'Link copied');
        }
      } catch (_) {
        try {
          const fallbackInput = document.createElement('textarea');
          fallbackInput.value = text;
          fallbackInput.style.position = 'fixed';
          fallbackInput.style.left = '-9999px';
          document.body.appendChild(fallbackInput);
          fallbackInput.focus();
          fallbackInput.select();
          document.execCommand('copy');
          document.body.removeChild(fallbackInput);
          if (triggerElement) {
            showCopyFlash(triggerElement, successMessage || 'Link copied');
          }
        } catch (_) {
          deps.setAuthStatus('Could not copy link.', 'err');
        }
      }
    }

    async function persistRecentItemToServer(item) {
      const response = await deps.apiFetch(deps.appendRunContext('/api/recent/items?limit=' + deps.recentItemsMax), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          itemType: item.type,
          itemKey: item.key,
          label: item.label,
          subtitle: item.subtitle || '',
          url: item.url || ''
        })
      });
      if (!response.ok) {
        throw new Error('HTTP ' + response.status);
      }
      const payload = await response.json();
      setRecentItems((Array.isArray(payload) ? payload : []).map(entry => ({
        type: String(entry.itemType || '').trim().toLowerCase(),
        key: String(entry.itemKey || '').trim(),
        label: String(entry.label || entry.itemKey || '').trim(),
        subtitle: String(entry.subtitle || '').trim(),
        url: String(entry.url || '').trim(),
        timestamp: Number(entry.lastAccessed || Date.now())
      })));
      renderRecentItems();
    }

    async function recordRecentItem(item) {
      if (!item || !item.type || !item.key || !item.label) {
        return;
      }
      const normalizedType = String(item.type).trim().toLowerCase();
      const normalizedKey = String(item.key).trim();
      if (!normalizedType || !normalizedKey) {
        return;
      }
      const nextItems = (getRecentItems() || []).filter(existing =>
        !(String(existing.type).trim().toLowerCase() === normalizedType && String(existing.key).trim() === normalizedKey)
      );
      nextItems.unshift({
        type: normalizedType,
        key: normalizedKey,
        label: String(item.label || normalizedKey).trim(),
        subtitle: String(item.subtitle || '').trim(),
        url: String(item.url || '').trim(),
        timestamp: Date.now()
      });
      setRecentItems(nextItems.slice(0, deps.recentItemsMax));
      saveRecentItems();
      renderRecentItems();
      if (deps.getCurrentUser()) {
        try {
          await persistRecentItemToServer(item);
        } catch (_) {
        }
      } else {
        saveRecentItems();
      }
    }

    function renderRecentItems() {
      if (!el.recentItemsContainer) {
        return;
      }
      if (!(getRecentItems() || []).length) {
        el.recentItemsContainer.innerHTML = '';
        updateRecentPanelVisibility();
        return;
      }
      el.recentItemsContainer.innerHTML = (getRecentItems() || []).map((item, index) => (
        '<div class="recent-item-card" role="button" tabindex="0" data-recent-index="' + index + '">' +
          '<button class="recent-bookmark-btn ' + (deps.isRecentItemBookmarked(item) ? 'active' : '') + '" type="button" data-recent-bookmark-index="' + index + '" title="' + (deps.isRecentItemBookmarked(item) ? 'Bookmarked' : 'Bookmark') + '">' +
            (deps.isRecentItemBookmarked(item) ? '★' : '☆') +
          '</button>' +
          '<div class="recent-item-type ' + (item.type === 'report' ? 'report' : 'mdrm') + '">' +
            '<span class="recent-item-type-icon" aria-hidden="true">' + (item.type === 'report' ? '📊' : '🧾') + '</span>' +
            '<span>' + deps.escapeHtml(item.type === 'report' ? 'Report' : 'MDRM') + '</span>' +
          '</div>' +
          '<div class="recent-item-title">' + deps.escapeHtml(item.label) + '</div>' +
          (item.subtitle ? '<div class="recent-item-subtitle">' + deps.escapeHtml(item.subtitle) + '</div>' : '') +
        '</div>'
      )).join('');
      updateRecentPanelVisibility();
    }

    function openRecentItem(index) {
      const item = (getRecentItems() || [])[index];
      if (!item) {
        return;
      }
      if (item.type === 'report') {
        deps.openOntologyInsight('report', item.key).catch(() => {});
        return;
      }
      if (item.type === 'mdrm') {
        deps.openOntologyInsight('mdrm', item.key).catch(() => {});
        return;
      }
      if (item.url) {
        window.location.href = item.url;
      }
    }

    function updateDiscoveryMetaCounts() {
      const gridApi = getDiscoveryGridApi();
      if (!gridApi) {
        return;
      }
      const total = (getLastDiscoveryRows() || []).length;
      const visible = gridApi.getDisplayedRowCount();
      setDiscoveryMeta(total === visible ? total + ' records found' : visible + ' of ' + total + ' records shown');
    }

    function pickRowValue(row, keys) {
      const byLower = {};
      Object.keys(row || {}).forEach(key => {
        byLower[key.toLowerCase()] = row[key];
      });
      for (const key of keys) {
        const value = byLower[key.toLowerCase()];
        if (value !== null && value !== undefined && String(value).trim() !== '') {
          return value;
        }
      }
      return '';
    }

    function buildDiscoveryRows(rows) {
      return rows.map(row => {
        const code = String(pickRowValue(row, ['mdrm_code', 'mdrm']) || '').trim();
        const descriptionRaw = String(pickRowValue(row, ['line_description', 'item_name', 'description', 'label']) || '').trim();
        const reportingForm = String(pickRowValue(row, ['reporting_form', 'report']) || '').trim();
        const rawType = String(pickRowValue(row, ['item_type', 'item_type_cd', 'itemtype', 'mdrm_type', 'type']) || '').trim();
        const typeCodeMap = {
          J: 'Projected',
          D: 'Derived',
          F: 'Financial/reported',
          R: 'Rate',
          S: 'Structure',
          E: 'Examination/supervision',
          P: 'Percentage'
        };
        const mdrmType = rawType.length === 1 && typeCodeMap[rawType.toUpperCase()]
          ? typeCodeMap[rawType.toUpperCase()]
          : rawType;
        return { code, descriptionRaw, reportingForm, mdrmType, bookmarked: false };
      });
    }

    function refreshDiscoveryBookmarkFlags() {
      setLastDiscoveryRows((getLastDiscoveryRows() || []).map(row => ({
        ...row,
        bookmarked: deps.isBookmarkedByTypeAndKey('mdrm', row.code)
      })));
      if (getDiscoveryGridApi()) {
        getDiscoveryGridApi().setGridOption('rowData', getLastDiscoveryRows());
      }
      deps.refreshReportingBookmarkFlags();
    }

    function applyDiscoveryGridTheme(theme) {
      if (!el.discoveryGrid) {
        return;
      }
      const dark = theme === 'dark';
      el.discoveryGrid.classList.toggle('ag-theme-quartz-dark', dark);
      el.discoveryGrid.classList.toggle('ag-theme-quartz', !dark);
    }

    function getDiscoverySearchState() {
      return {
        q: String(el.discoveryQueryInput?.value || '').trim(),
        fc: String(el.filterCodeInput?.value || '').trim(),
        fd: String(el.filterDescriptionInput?.value || '').trim(),
        fr: String(el.filterReportingFormInput?.value || '').trim(),
        qf: String(el.discoveryQuickFilterInput?.value || '').trim(),
        runId: deps.getRunContextId()
      };
    }

    function applyDiscoverySearchState(state) {
      const nextState = state || {};
      if (el.discoveryQueryInput) el.discoveryQueryInput.value = String(nextState.q || '').trim();
      if (el.filterCodeInput) el.filterCodeInput.value = String(nextState.fc || '').trim();
      if (el.filterDescriptionInput) el.filterDescriptionInput.value = String(nextState.fd || '').trim();
      if (el.filterReportingFormInput) el.filterReportingFormInput.value = String(nextState.fr || '').trim();
      if (el.discoveryQuickFilterInput) el.discoveryQuickFilterInput.value = String(nextState.qf || '').trim();
    }

    function buildDiscoveryReturnUrl(state) {
      const params = new URLSearchParams();
      params.set('view', 'view-discovery');
      const nextState = state || getDiscoverySearchState();
      const q = String(nextState.q || '').trim();
      const codeFilter = String(nextState.fc || '').trim();
      const descriptionFilter = String(nextState.fd || '').trim();
      const reportingFilter = String(nextState.fr || '').trim();
      const quickFilter = String(nextState.qf || '').trim();
      const runId = Number(nextState.runId || 0);
      if (q) params.set('q', q);
      if (codeFilter) params.set('fc', codeFilter);
      if (descriptionFilter) params.set('fd', descriptionFilter);
      if (reportingFilter) params.set('fr', reportingFilter);
      if (quickFilter) params.set('qf', quickFilter);
      if (runId > 0) params.set('runId', String(runId));
      return '/index.html?' + params.toString();
    }

    function buildReportDeepLink(reportingForm) {
      const form = String(reportingForm || '').trim();
      const params = new URLSearchParams();
      params.set('view', 'view-reporting');
      if (form) {
        params.set('report', form);
      }
      const runId = deps.getRunContextId();
      if (runId) {
        params.set('runId', String(runId));
      }
      return window.location.origin + '/index.html?' + params.toString();
    }

    function buildMdrmProfileUrl(mdrmCode) {
      const code = String(mdrmCode || '').trim();
      if (!code) {
        return '/mdrm.html';
      }
      const params = new URLSearchParams();
      params.set('mdrm', code);
      const runId = deps.getRunContextId();
      if (runId) {
        params.set('runId', String(runId));
      }
      params.set('returnTo', buildDiscoveryReturnUrl());
      return '/mdrm.html?' + params.toString();
    }

    function initDiscoveryGrid() {
      if (getDiscoveryGridApi() || !el.discoveryGrid || !window.agGrid) {
        return;
      }
      setDiscoveryGridApi(window.agGrid.createGrid(el.discoveryGrid, {
        defaultColDef: {
          sortable: true,
          filter: true,
          resizable: true,
          floatingFilter: false
        },
        pagination: true,
        paginationPageSize: 20,
        paginationPageSizeSelector: [20, 50, 100],
        suppressCellFocus: true,
        rowHeight: 30,
        headerHeight: 34,
        animateRows: true,
        tooltipShowDelay: 150,
        tooltipMouseTrack: true,
        onFilterChanged: () => updateDiscoveryMetaCounts(),
        onCellClicked: async params => {
          if (!params || !params.column) {
            return;
          }
          const columnId = params.column.getColId();
          if (columnId === 'code') {
            const code = String(params.data && params.data.code ? params.data.code : '').trim();
            if (code) {
              const reportingForm = String(params.data && params.data.reportingForm ? params.data.reportingForm : '').trim();
              recordRecentItem({
                type: 'mdrm',
                key: code,
                label: code,
                subtitle: reportingForm ? 'Reporting form: ' + reportingForm : '',
                url: buildMdrmProfileUrl(code)
              });
              deps.openOntologyInsight('mdrm', code).catch(() => {});
            }
            return;
          }
          if (columnId === 'reportingForm') {
            const report = String(params.data && params.data.reportingForm ? params.data.reportingForm : '').trim();
            if (report) {
              recordRecentItem({
                type: 'report',
                key: report,
                label: report,
                subtitle: 'Reporting form',
                url: buildReportDeepLink(report)
              });
              deps.openOntologyInsight('report', report).catch(() => {});
            }
            return;
          }
          if (columnId !== 'bookmarkAction') {
            return;
          }
          const row = params.data || {};
          const code = String(row.code || '').trim();
          if (!code) {
            return;
          }
          try {
            if (!deps.getCurrentUser()) {
              deps.setAuthStatus('Login required to save bookmarks.', 'err');
              if (el.profileMenu && el.profileMenuBtn) {
                el.profileMenu.classList.add('open');
                el.profileMenuBtn.setAttribute('aria-expanded', 'true');
              }
              return;
            }
            if (row.bookmarked) {
              await deps.removeBookmarkByItemKey(code);
              return;
            }
            const subtitle = [row.descriptionRaw || '', row.reportingForm ? 'Form ' + row.reportingForm : ''].filter(Boolean).join(' | ');
            deps.showSaveBookmarkModal(deps.buildBookmarkPayload(code, code, subtitle, buildMdrmProfileUrl(code)));
          } catch (_) {
            deps.setAuthStatus('Bookmark action failed. Please retry.', 'err');
          }
        },
        columnDefs: [
          {
            headerName: 'MDRM Code',
            field: 'code',
            minWidth: 145,
            maxWidth: 220,
            cellRenderer: params => {
              const value = String(params.value || '').trim();
              return value ? '<button class="ontology-ref-link mono" type="button">' + deps.escapeHtml(value) + '</button>' : '-';
            }
          },
          {
            headerName: 'Description',
            field: 'descriptionRaw',
            flex: 1.4,
            minWidth: 320,
            wrapText: true,
            autoHeight: true,
            tooltipValueGetter: params => {
              const value = String(params.value || '');
              return value.length > 100 ? value : '';
            },
            cellRenderer: params => deps.renderExpandableText(params.value || '-', { maxLength: 100 })
          },
          {
            headerName: 'Reporting Form',
            field: 'reportingForm',
            minWidth: 170,
            maxWidth: 240,
            cellRenderer: params => {
              const value = String(params.value || '').trim();
              return value ? '<button class="ontology-ref-link" type="button">' + deps.escapeHtml(value) + '</button>' : '-';
            }
          },
          {
            headerName: 'MDRM Type',
            field: 'mdrmType',
            minWidth: 190,
            maxWidth: 260,
            valueFormatter: params => String(params.value || '-')
          },
          {
            headerName: 'Favorite',
            colId: 'bookmarkAction',
            field: 'bookmarked',
            minWidth: 92,
            maxWidth: 126,
            sortable: true,
            filter: false,
            resizable: false,
            cellStyle: { textAlign: 'center' },
            comparator: (valueA, valueB) => Number(Boolean(valueA)) - Number(Boolean(valueB)),
            cellRenderer: params => {
              const code = String(params.data && params.data.code ? params.data.code : '').trim();
              if (!code) {
                return '-';
              }
              const isBookmarked = !!(params.data && params.data.bookmarked);
              const description = String(params.data && params.data.descriptionRaw ? params.data.descriptionRaw : '').trim();
              const report = String(params.data && params.data.reportingForm ? params.data.reportingForm : '').trim();
              const subtitle = [description, report ? 'Form ' + report : ''].filter(Boolean).join(' | ');
              return '<button class="bookmark-star-btn bookmark-item-btn ' + (isBookmarked ? 'active' : '') + '"' +
                ' type="button"' +
                ' title="' + (isBookmarked ? 'Bookmarked' : 'Save bookmark') + '"' +
                ' data-bookmarked="' + (isBookmarked ? 'true' : 'false') + '"' +
                ' data-item-type="mdrm"' +
                ' data-item-key="' + deps.escapeHtml(code) + '"' +
                ' data-title="' + deps.escapeHtml(code) + '"' +
                ' data-subtitle="' + deps.escapeHtml(subtitle) + '"' +
                ' data-url="' + deps.escapeHtml(buildMdrmProfileUrl(code)) + '">' +
                (isBookmarked ? '★' : '☆') +
              '</button>';
            }
          }
        ],
        rowData: []
      }));
      applyDiscoveryGridTheme(document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light');
      window.addEventListener('resize', () => {
        if (getDiscoveryGridApi()) {
          getDiscoveryGridApi().sizeColumnsToFit();
        }
      });
    }

    function renderDiscoveryRows(rows) {
      initDiscoveryGrid();
      const codeFilter = String(el.filterCodeInput.value || '').trim().toLowerCase();
      const descriptionFilter = String(el.filterDescriptionInput.value || '').trim().toLowerCase();
      const reportingFormFilter = String(el.filterReportingFormInput.value || '').trim().toLowerCase();
      const filteredRows = rows.filter(row => {
        if (codeFilter && !row.code.toLowerCase().includes(codeFilter)) {
          return false;
        }
        if (descriptionFilter && !row.descriptionRaw.toLowerCase().includes(descriptionFilter)) {
          return false;
        }
        if (reportingFormFilter && !row.reportingForm.toLowerCase().includes(reportingFormFilter)) {
          return false;
        }
        return true;
      });

      if (!rows.length) {
        if (getDiscoveryGridApi()) {
          getDiscoveryGridApi().setGridOption('rowData', []);
        }
        setDiscoveryResultsVisible(true);
        if (el.discoveryGrid) {
          el.discoveryGrid.style.display = 'none';
        }
        setDiscoveryMeta('No records found');
        return;
      }

      if (!filteredRows.length) {
        if (getDiscoveryGridApi()) {
          getDiscoveryGridApi().setGridOption('rowData', []);
        }
        setDiscoveryResultsVisible(true);
        if (el.discoveryGrid) {
          el.discoveryGrid.style.display = 'none';
        }
        setDiscoveryMeta('No records match current filters');
        return;
      }

      setDiscoveryResultsVisible(true);
      if (el.discoveryGrid) {
        el.discoveryGrid.style.display = 'block';
      }
      if (getDiscoveryGridApi()) {
        const firstPassRows = [...filteredRows].sort((a, b) => {
          const favDelta = Number(Boolean(b.bookmarked)) - Number(Boolean(a.bookmarked));
          if (favDelta !== 0) {
            return favDelta;
          }
          return String(a.code || '').localeCompare(String(b.code || ''), undefined, { sensitivity: 'base' });
        });
        getDiscoveryGridApi().setGridOption('rowData', firstPassRows);
        setTimeout(() => getDiscoveryGridApi() && getDiscoveryGridApi().sizeColumnsToFit(), 0);
        updateDiscoveryMetaCounts();
      }
    }

    function renderDiscoveryResults(payload) {
      const table = payload && payload.results ? payload.results : null;
      const rows = table && Array.isArray(table.rows) ? table.rows : [];
      setLastDiscoveryRows(buildDiscoveryRows(rows));
      refreshDiscoveryBookmarkFlags();
      if (el.discoveryFiltersRow) {
        el.discoveryFiltersRow.style.display = 'none';
      }
      renderDiscoveryRows(getLastDiscoveryRows());
    }

    async function runDiscoverySearch() {
      const query = String(el.discoveryQueryInput.value || '').trim();
      if (!query) {
        setDiscoveryResultsVisible(false);
        deps.syncCopyLinkButtons();
        return;
      }
      upsertRecentSearch(query);
      el.discoverySearchBtn.disabled = true;
      setDiscoveryResultsVisible(true);
      setDiscoveryMeta('Searching...');
      if (el.discoveryQuickFilterInput) {
        el.discoveryQuickFilterInput.value = '';
      }
      if (getDiscoveryGridApi()) {
        getDiscoveryGridApi().setGridOption('quickFilterText', '');
        getDiscoveryGridApi().setGridOption('rowData', []);
      }
      if (el.discoveryGrid) {
        el.discoveryGrid.style.display = 'none';
      }
      try {
        const response = await fetch(deps.appendRunContext('/api/mdrm/semantic-search?q=' + encodeURIComponent(query)));
        const payload = await response.json();
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        renderDiscoveryResults(payload);
      } catch (error) {
        setDiscoveryMeta('Search failed: ' + error.message);
      } finally {
        el.discoverySearchBtn.disabled = false;
        deps.syncCopyLinkButtons();
      }
    }

    return {
      setDiscoveryMeta,
      setDiscoveryResultsVisible,
      formatCount,
      updateRecentPanelVisibility,
      loadRecentItemsFromLocal,
      loadRecentItems,
      saveRecentItems,
      getSearchMemoryStorage,
      loadSearchMemory,
      persistSearchMemory,
      normalizeSearchQuery,
      upsertRecentSearch,
      renderRecentSearches,
      renderSavedSearches,
      renderSearchMemory,
      applyRecentSearchByIndex,
      applySavedSearchByIndex,
      clearRecentSearches,
      removeSavedSearchByIndex,
      saveCurrentSearch,
      showCopyFlash,
      copyText,
      persistRecentItemToServer,
      recordRecentItem,
      renderRecentItems,
      openRecentItem,
      updateDiscoveryMetaCounts,
      pickRowValue,
      buildDiscoveryRows,
      refreshDiscoveryBookmarkFlags,
      applyDiscoveryGridTheme,
      getDiscoverySearchState,
      applyDiscoverySearchState,
      buildDiscoveryReturnUrl,
      buildReportDeepLink,
      buildMdrmProfileUrl,
      initDiscoveryGrid,
      renderDiscoveryRows,
      renderDiscoveryResults,
      runDiscoverySearch
    };
  }

  window.ProjectAiUiModules = Object.assign(window.ProjectAiUiModules || {}, {
    createDiscoveryManager
  });
})();
