(function () {
  function createOntologyManager(deps) {
    const el = deps.elements;
    const ONTOLOGY_PANE_HEIGHT_KEY = 'mdrm-ontology-top-pane-height-v1';

    function getOntologyInfoNoteEl() {
      return deps.getOntologyInfoNoteEl();
    }

    function setOntologyInfoNoteEl(value) {
      deps.setOntologyInfoNoteEl(value);
    }

    function getOntologyGraphPayload() {
      return deps.getOntologyGraphPayload();
    }

    function setOntologyGraphPayload(value) {
      deps.setOntologyGraphPayload(value);
    }

    function getOntologyBaseSummaryPayload() {
      return deps.getOntologyBaseSummaryPayload();
    }

    function setOntologyBaseSummaryPayload(value) {
      deps.setOntologyBaseSummaryPayload(value);
    }

    function getOntologyGraphSimulation() {
      return deps.getOntologyGraphSimulation();
    }

    function setOntologyGraphSimulation(value) {
      deps.setOntologyGraphSimulation(value);
    }

    function getOntologyGraphSvg() {
      return deps.getOntologyGraphSvg();
    }

    function setOntologyGraphSvg(value) {
      deps.setOntologyGraphSvg(value);
    }

    function getOntologyGraphNodeSelection() {
      return deps.getOntologyGraphNodeSelection();
    }

    function setOntologyGraphNodeSelection(value) {
      deps.setOntologyGraphNodeSelection(value);
    }

    function getOntologyGraphLinkSelection() {
      return deps.getOntologyGraphLinkSelection();
    }

    function setOntologyGraphLinkSelection(value) {
      deps.setOntologyGraphLinkSelection(value);
    }

    function getOntologyGraphLinkLabelSelection() {
      return deps.getOntologyGraphLinkLabelSelection();
    }

    function setOntologyGraphLinkLabelSelection(value) {
      deps.setOntologyGraphLinkLabelSelection(value);
    }

    function getOntologySelectedNodeId() {
      return deps.getOntologySelectedNodeId();
    }

    function setOntologySelectedNodeId(value) {
      deps.setOntologySelectedNodeId(value);
    }

    function getOntologyNeighborMap() {
      return deps.getOntologyNeighborMap();
    }

    function setOntologyNeighborMap(value) {
      deps.setOntologyNeighborMap(value);
    }

    function getOntologyNodeById() {
      return deps.getOntologyNodeById();
    }

    function setOntologyNodeById(value) {
      deps.setOntologyNodeById(value);
    }

    function getOntologyLoadedContextRunId() {
      return deps.getOntologyLoadedContextRunId();
    }

    function setOntologyLoadedContextRunId(value) {
      deps.setOntologyLoadedContextRunId(value);
    }

    function getOntologySelectedReportsState() {
      return deps.getOntologySelectedReportsState();
    }

    function setOntologySelectedReportsState(value) {
      deps.setOntologySelectedReportsState(value);
    }

    function getOntologySelectedMdrmsState() {
      return deps.getOntologySelectedMdrmsState();
    }

    function setOntologySelectedMdrmsState(value) {
      deps.setOntologySelectedMdrmsState(value);
    }

    function getOntologySelectorMode() {
      return deps.getOntologySelectorMode();
    }

    function setOntologySelectorMode(value) {
      deps.setOntologySelectorMode(value);
    }

    function getOntologySelectorOptions() {
      return deps.getOntologySelectorOptions();
    }

    function setOntologySelectorOptions(value) {
      deps.setOntologySelectorOptions(value);
    }

    function getOntologySelectorWorkingSet() {
      return deps.getOntologySelectorWorkingSet();
    }

    function setOntologySelectorWorkingSet(value) {
      deps.setOntologySelectorWorkingSet(value);
    }

    function getOntologyFocusedReport() {
      return deps.getOntologyFocusedReport();
    }

    function setOntologyFocusedReport(value) {
      deps.setOntologyFocusedReport(value);
    }

    function getOntologyFocusedMdrm() {
      return deps.getOntologyFocusedMdrm();
    }

    function setOntologyFocusedMdrm(value) {
      deps.setOntologyFocusedMdrm(value);
    }

    function getOntologyManageModal() {
      return deps.getOntologyManageModal();
    }

    function setOntologyManageModal(value) {
      deps.setOntologyManageModal(value);
    }

    function getOntologyManageMdrmOptionList() {
      return deps.getOntologyManageMdrmOptionList();
    }

    function setOntologyManageMdrmOptionList(value) {
      deps.setOntologyManageMdrmOptionList(value);
    }

    function getOntologyRuleSummaryPayload() {
      return deps.getOntologyRuleSummaryPayload();
    }

    function setOntologyRuleSummaryPayload(value) {
      deps.setOntologyRuleSummaryPayload(value);
    }

    function resetOntologyScopeState() {
      setOntologySelectedReportsState([]);
      setOntologySelectedMdrmsState([]);
      setOntologyFocusedReport('');
      setOntologyFocusedMdrm('');
    }

    function getEffectiveSelectedReports() {
      const scoped = (getOntologySelectedReportsState() || []).map(value => String(value || '').trim()).filter(Boolean);
      if (scoped.length > 0) {
        return scoped;
      }
      const focused = String(getOntologyFocusedReport() || '').trim();
      return focused ? [focused] : [];
    }

    function getEffectiveSelectedMdrms() {
      const scoped = (getOntologySelectedMdrmsState() || []).map(value => String(value || '').trim().toUpperCase()).filter(Boolean);
      if (scoped.length > 0) {
        return scoped.filter(value => isValidOntologyMdrmCode(value));
      }
      const focused = String(getOntologyFocusedMdrm() || '').trim().toUpperCase();
      return focused && isValidOntologyMdrmCode(focused) ? [focused] : [];
    }

    function setActiveOntologyInspectorTab(tabName) {
      const normalized = String(tabName || 'summary').trim().toLowerCase();
      const tabs = [
        ['summary', el.ontologyTabSummaryBtn, el.ontologySummaryTab],
        ['reports', el.ontologyTabReportsBtn, el.ontologyReportsTab],
        ['mdrms', el.ontologyTabMdrmsBtn, el.ontologyMdrmsTab],
        ['rules', el.ontologyTabRulesBtn, el.ontologyRulesTab]
      ];
      tabs.forEach(([name, button, panel]) => {
        const isActive = name === normalized;
        if (button) button.classList.toggle('active', isActive);
        if (panel) {
          panel.classList.toggle('active', isActive);
          panel.style.display = isActive ? '' : 'none';
        }
      });
    }

    function clampOntologyTopPaneHeight(height) {
      const workspaceHeight = Number(el.ontologyWorkspace?.clientHeight || 0);
      const handleHeight = Number(el.ontologyResizeHandle?.offsetHeight || 18);
      const minTopHeight = 320;
      const minBottomHeight = 220;
      if (!workspaceHeight) {
        return Math.max(minTopHeight, Number(height || 0) || 0);
      }
      const maxTopHeight = Math.max(minTopHeight, workspaceHeight - handleHeight - minBottomHeight);
      return Math.min(maxTopHeight, Math.max(minTopHeight, Number(height || 0) || minTopHeight));
    }

    function applyOntologyTopPaneHeight(height, persist = true) {
      if (!el.ontologyTopPane) {
        return;
      }
      const clampedHeight = clampOntologyTopPaneHeight(height);
      el.ontologyTopPane.style.flexBasis = `${clampedHeight}px`;
      el.ontologyTopPane.style.height = `${clampedHeight}px`;
      if (persist) {
        try {
          window.localStorage?.setItem(ONTOLOGY_PANE_HEIGHT_KEY, String(Math.round(clampedHeight)));
        } catch (_) {
          // ignore storage failures
        }
      }
    }

    function refreshOntologyAfterResize() {
      const payload = getOntologyGraphPayload() || getOntologyBaseSummaryPayload();
      if (payload) {
        renderOntologySummaryGraph(payload);
        renderOntologyDetail(getOntologySelectedNodeId() ? getOntologyNodeById().get(getOntologySelectedNodeId()) || null : null);
        renderOntologyReferenceTables();
      }
    }

    function initOntologyPaneResize() {
      if (!el.ontologyWorkspace || !el.ontologyTopPane || !el.ontologyResizeHandle) {
        return;
      }
      const defaultHeight = Math.round((Number(el.ontologyWorkspace.clientHeight || 880)) * 0.56);
      let storedHeight = defaultHeight;
      try {
        const saved = Number(window.localStorage?.getItem(ONTOLOGY_PANE_HEIGHT_KEY) || 0);
        if (saved > 0) {
          storedHeight = saved;
        }
      } catch (_) {
        storedHeight = defaultHeight;
      }
      applyOntologyTopPaneHeight(storedHeight, false);

      let dragState = null;
      const stopDrag = () => {
        if (!dragState) {
          return;
        }
        dragState = null;
        document.body.classList.remove('ontology-resizing');
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        refreshOntologyAfterResize();
      };

      const onPointerMove = event => {
        if (!dragState) {
          return;
        }
        const nextHeight = dragState.startHeight + (event.clientY - dragState.startY);
        applyOntologyTopPaneHeight(nextHeight, true);
      };

      el.ontologyResizeHandle.addEventListener('pointerdown', event => {
        if (window.matchMedia('(max-width: 980px)').matches) {
          return;
        }
        dragState = {
          startY: event.clientY,
          startHeight: el.ontologyTopPane.getBoundingClientRect().height
        };
        document.body.classList.add('ontology-resizing');
        document.body.style.cursor = 'row-resize';
        document.body.style.userSelect = 'none';
        event.preventDefault();
      });
      window.addEventListener('pointermove', onPointerMove);
      window.addEventListener('pointerup', stopDrag);
      window.addEventListener('pointercancel', stopDrag);
      window.addEventListener('resize', () => {
        applyOntologyTopPaneHeight(el.ontologyTopPane.getBoundingClientRect().height || defaultHeight, false);
        refreshOntologyAfterResize();
      });
    }

    function renderOntologyScopeChips() {
      if (!el.ontologyScopeChips) {
        return;
      }
      const reports = getEffectiveSelectedReports();
      const mdrms = getEffectiveSelectedMdrms();
      const rulePayload = getOntologyRuleSummaryPayload();
      const chips = [];
      if (reports.length > 0) {
        chips.push(`<span class="ontology-scope-chip report">Report: ${deps.escapeHtml(reports.join(', '))}</span>`);
      }
      if (mdrms.length > 0) {
        chips.push(`<span class="ontology-scope-chip mdrm">MDRM: ${deps.escapeHtml(mdrms.join(', '))}</span>`);
      }
      if (rulePayload) {
        chips.push(`<span class="ontology-scope-chip rule">Rules: ${deps.escapeHtml(deps.formatCount(rulePayload.totalRules || 0))}</span>`);
      } else if (!reports.length && !mdrms.length) {
        chips.push('<span class="ontology-scope-chip">Global ontology scope</span>');
      }
      el.ontologyScopeChips.innerHTML = chips.join('');
    }

    function buildOntologyRulesHeading(payload) {
      const reports = getEffectiveSelectedReports();
      const mdrms = getEffectiveSelectedMdrms();
      if (mdrms.length === 1 && reports.length === 1) {
        return `Rules for MDRM ${mdrms[0]} in Report ${reports[0]}`;
      }
      if (mdrms.length === 1) {
        return `Rules for MDRM ${mdrms[0]}`;
      }
      if (mdrms.length > 1 && reports.length === 1) {
        return `Rules for ${deps.formatCount(mdrms.length)} MDRMs in Report ${reports[0]}`;
      }
      if (reports.length === 1) {
        return `Rules for Report ${reports[0]}`;
      }
      if (reports.length > 1) {
        return `Rules for ${deps.formatCount(reports.length)} Reports`;
      }
      return payload?.scopeValue ? `Rules for ${payload.scopeValue}` : 'Rules for current ontology scope';
    }

    function buildOntologySelectionSentence(node) {
      const reports = getEffectiveSelectedReports();
      const mdrms = getEffectiveSelectedMdrms();
      const graphPayload = getOntologyGraphPayload() || getOntologyBaseSummaryPayload() || {};
      const rulePayload = getOntologyRuleSummaryPayload();
      const reportCount = reports.length || Number(graphPayload.reportCount || 0);
      const mdrmCount = mdrms.length || Number(graphPayload.activeMdrmCount || graphPayload.mdrmCount || 0);
      const ruleCount = rulePayload ? Number(rulePayload.totalRules || 0) : Number(graphPayload.ruleCount || 0);
      const discrepancyCount = rulePayload ? Number(rulePayload.discrepancyCount || 0) : Number(graphPayload.ruleDiscrepancyCount || 0);
      const discrepancyText = discrepancyCount > 0
        ? discrepancyCount === 1
          ? ` ${deps.formatCount(discrepancyCount)} discrepancy is flagged in this scope.`
          : ` ${deps.formatCount(discrepancyCount)} discrepancies are flagged in this scope.`
        : '';

      if (!node) {
        if (reports.length === 1 && mdrms.length === 1) {
          return `You are viewing MDRM ${mdrms[0]} within report ${reports[0]}. This scope currently includes ${deps.formatCount(ruleCount)} rules.${discrepancyText}`;
        }
        if (reports.length === 1 && mdrms.length > 1) {
          return `You are viewing ${deps.formatCount(mdrms.length)} MDRMs within report ${reports[0]}. This scope currently includes ${deps.formatCount(ruleCount)} rules.${discrepancyText}`;
        }
        if (reports.length === 1) {
          return `You are viewing report ${reports[0]}. This scope currently includes ${deps.formatCount(mdrmCount)} active MDRMs and ${deps.formatCount(ruleCount)} rules.${discrepancyText}`;
        }
        if (mdrms.length === 1) {
          return `You are viewing MDRM ${mdrms[0]}. This scope currently includes ${deps.formatCount(ruleCount)} rules across ${deps.formatCount(reportCount)} reports.${discrepancyText}`;
        }
        if (mdrms.length > 1) {
          return `You are viewing ${deps.formatCount(mdrms.length)} MDRMs across ${deps.formatCount(reportCount)} reports. This scope currently includes ${deps.formatCount(ruleCount)} rules.${discrepancyText}`;
        }
        return `You are viewing the global ontology summary across ${deps.formatCount(reportCount)} reports, ${deps.formatCount(mdrmCount)} active MDRMs, and ${deps.formatCount(ruleCount)} rules. Select a node to inspect its current scope.${discrepancyText}`;
      }

      const category = normalizeOntologyCategory(node.category);
      if (category === 'REPORT') {
        return `You selected report ${node.label || '-'}. Use the tabs below to inspect its MDRMs and associated rules.`;
      }
      if (category === 'MDRM') {
        const reportText = reports.length === 1 ? ` within report ${reports[0]}` : '';
        return `You selected MDRM ${node.label || '-'}${reportText}. Use the Rules tab to review the rules and dependencies attached to this MDRM.`;
      }
      if (category === 'RULE') {
        return `You selected the rules summary for the current ontology scope. Use the Rules tab to inspect the matching rule records.`;
      }
      if (category === 'MDRM_TYPE') {
        return `You selected MDRM type ${node.label || '-'}. Use the MDRMs tab to inspect the members in this type bucket.`;
      }
      return `You selected ${node.label || 'this node'}.`;
    }

    function renderOntologyRulesTab(payload) {
      if (!el.ontologyRulesContainer || !el.ontologyRulesMeta) {
        return;
      }
      const heading = buildOntologyRulesHeading(payload);
      if (!payload) {
        el.ontologyRulesMeta.textContent = heading;
        el.ontologyRulesContainer.innerHTML = '<div class="text-muted small p-3">Select a report or MDRM on the ontology graph to inspect associated rules.</div>';
        return;
      }
      const rules = Array.isArray(payload.rules) ? payload.rules : [];
      el.ontologyRulesMeta.textContent = `${heading} | rules=${deps.formatCount(payload.totalRules || 0)} | dependencies=${deps.formatCount(payload.totalDependencies || 0)} | discrepancies=${deps.formatCount(payload.discrepancyCount || 0)}`;
      if (!rules.length) {
        el.ontologyRulesContainer.innerHTML = '<div class="text-muted small p-3">No rules found for the current ontology scope.</div>';
        return;
      }
      const rows = rules.slice(0, 150).map(rule => `
        <tr>
          <td>
            <div class="fw-semibold">${deps.escapeHtml(rule.ruleNumber || String(rule.ruleId || '-'))}</div>
            <div class="small text-muted">${deps.escapeHtml((rule.ruleText || rule.ruleExpression || '-').slice(0, 180))}</div>
          </td>
          <td>${deps.escapeHtml(rule.scheduleName || '-')}</td>
          <td class="mono">${deps.escapeHtml(rule.primaryMdrmCode || '-')}</td>
          <td>${deps.statusCapsule(rule.lineageStatus || 'VALID')}</td>
          <td class="text-end">
            <button class="btn btn-sm btn-outline-primary ontology-open-rule-btn" type="button" data-rule-id="${deps.escapeHtml(rule.ruleId)}">Open</button>
          </td>
        </tr>
      `).join('');
      el.ontologyRulesContainer.innerHTML = `
        <table class="table table-sm table-striped table-hover mb-0">
          <thead>
            <tr>
              <th>Rule</th>
              <th>Schedule</th>
              <th>Primary MDRM</th>
              <th>Status</th>
              <th></th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      `;
      el.ontologyRulesContainer.querySelectorAll('.ontology-open-rule-btn').forEach(button => {
        button.addEventListener('click', () => {
          const ruleId = String(button.getAttribute('data-rule-id') || '').trim();
          if (!ruleId) return;
          openOntologyInsight('rule', ruleId);
        });
      });
    }

    function getOntologyMode() {
      const value = String(el.ontologyModeSelect?.value || 'structure').trim().toLowerCase();
      return value === 'rules' ? 'rules' : 'structure';
    }

    function setOntologyStatus(message, tone = 'neutral') {
      if (!el.ontologyGraphHost) {
        return;
      }
      const cssTone = tone === 'err' ? 'err' : tone === 'ok' ? 'ok' : 'neutral';
      let statusEl = document.getElementById('ontologyGraphStatus');
      if (!statusEl) {
        statusEl = document.createElement('div');
        statusEl.id = 'ontologyGraphStatus';
        statusEl.className = 'ontology-graph-status neutral';
        el.ontologyGraphHost.appendChild(statusEl);
      }
      statusEl.className = `ontology-graph-status ${cssTone}`;
      statusEl.textContent = String(message || '').trim();
    }

    function normalizeOntologyCategory(value) {
      const raw = String(value || '').toUpperCase();
      if (raw === 'REPORT' || raw === 'MDRM' || raw === 'MDRM_TYPE' || raw === 'RULE') {
        return raw;
      }
      return 'UNKNOWN';
    }

    function ontologyNodeRadius(node) {
      const category = normalizeOntologyCategory(node && node.category);
      if (category === 'REPORT') return 19;
      if (category === 'MDRM') return 12;
      if (category === 'MDRM_TYPE') return 16;
      if (category === 'RULE') return 15;
      return 11;
    }

    function shortenOntologyLabel(label, category) {
      const text = String(label || '').trim();
      if (!text) return '-';
      const normalized = normalizeOntologyCategory(category);
      if (normalized === 'MDRM') return text.toUpperCase();
      if (normalized === 'RULE') return text.length > 18 ? `${text.slice(0, 18)}…` : text;
      if (normalized === 'MDRM_TYPE') return text.length > 14 ? `${text.slice(0, 14)}…` : text;
      return text.length > 10 ? `${text.slice(0, 10)}…` : text;
    }

    function ontologyNodeClass(node) {
      const category = normalizeOntologyCategory(node && node.category).toLowerCase();
      const status = String(node && node.status ? node.status : '').toLowerCase();
      const statusClass = status === 'active' ? 'is-active' : status === 'inactive' ? 'is-inactive' : '';
      return `ontology-node ontology-node-${category} ${statusClass}`.trim();
    }

    function buildOntologyUrl(path, { summaryOnly = false, includeReportFilters = true, includeMdrmFilters = true } = {}) {
      const params = new URLSearchParams();
      const runId = deps.getRunContextId();
      if (runId) {
        params.set('runId', String(runId));
      }
      if (summaryOnly) {
        params.set('summaryOnly', 'true');
      }
      if (includeReportFilters) {
        (getOntologySelectedReportsState() || []).forEach(report => params.append('reportingForm', report));
      }
      if (includeMdrmFilters) {
        (getOntologySelectedMdrmsState() || []).forEach(mdrm => params.append('mdrm', mdrm));
      }
      const query = params.toString();
      return query ? `${path}?${query}` : path;
    }

    function buildOntologyBaseSummaryUrl() {
      const params = new URLSearchParams();
      const runId = deps.getRunContextId();
      if (runId) {
        params.set('runId', String(runId));
      }
      params.set('summaryOnly', 'true');
      const query = params.toString();
      return query ? `/api/mdrm/ontology-graph?${query}` : '/api/mdrm/ontology-graph?summaryOnly=true';
    }

    function renderOntologyReferenceTables() {
      const selectedReports = getEffectiveSelectedReports();
      const selectedMdrms = getEffectiveSelectedMdrms();

      if (el.ontologyReportsTable) {
        if (!selectedReports.length) {
          el.ontologyReportsTable.innerHTML = '<div class="text-muted small">No report selected on graph.</div>';
        } else {
          const rows = selectedReports.slice(0, 300).map(report => `
            <tr class="is-selected">
              <td>
                <button class="ontology-ref-link" type="button" data-ontology-item-type="report" data-ontology-item-value="${deps.escapeHtml(report)}">${deps.escapeHtml(report)}</button>
              </td>
            </tr>
          `).join('');
          el.ontologyReportsTable.innerHTML = `
            <div class="ontology-reference-meta small text-muted mb-1">${deps.formatCount(selectedReports.length)} selected</div>
            <table class="table table-sm table-striped mb-0">
              <thead><tr><th>Report</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>
          `;
        }
      }

      if (el.ontologyMdrmsTable) {
        if (!selectedMdrms.length) {
          el.ontologyMdrmsTable.innerHTML = '<div class="text-muted small">No MDRM selected on graph scope.</div>';
        } else {
          const rows = selectedMdrms.slice(0, 500).map(mdrm => `
            <tr class="is-selected">
              <td class="mono">
                <button class="ontology-ref-link mono" type="button" data-ontology-item-type="mdrm" data-ontology-item-value="${deps.escapeHtml(mdrm)}">${deps.escapeHtml(mdrm)}</button>
              </td>
            </tr>
          `).join('');
          el.ontologyMdrmsTable.innerHTML = `
            <div class="ontology-reference-meta small text-muted mb-1">${deps.formatCount(selectedMdrms.length)} selected</div>
            <table class="table table-sm table-striped mb-0">
              <thead><tr><th>MDRM</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>
          `;
        }
      }
      renderOntologyScopeChips();
      renderOntologyRulesTab(getOntologyRuleSummaryPayload());
      syncOntologyManageActions();
    }

    function openOntologyInsightShell(title, bodyHtml) {
      if (el.ontologyInsightTitle) el.ontologyInsightTitle.textContent = title;
      if (el.ontologyInsightBody) el.ontologyInsightBody.innerHTML = bodyHtml || '';
      if (el.ontologyInsightBackdrop) el.ontologyInsightBackdrop.hidden = false;
      if (el.ontologyInsightDrawer) {
        el.ontologyInsightDrawer.classList.add('open');
        el.ontologyInsightDrawer.setAttribute('aria-hidden', 'false');
      }
    }

    function closeOntologyInsight() {
      if (el.ontologyInsightBackdrop) el.ontologyInsightBackdrop.hidden = true;
      if (el.ontologyInsightDrawer) {
        el.ontologyInsightDrawer.classList.remove('open');
        el.ontologyInsightDrawer.setAttribute('aria-hidden', 'true');
      }
    }

    async function loadOntologyReportInsight(reportingForm) {
      const encoded = encodeURIComponent(reportingForm);
      const [historyRes, metadataRes] = await Promise.all([
        fetch(deps.appendRunContext(`/api/mdrm/run-history?reportingForm=${encoded}`)),
        fetch(`/api/mdrm/report-metadata?reportingForm=${encoded}`)
      ]);
      const historyPayload = await historyRes.json().catch(() => ({}));
      const metadataPayload = await metadataRes.json().catch(() => ({}));
      if (!historyRes.ok) {
        throw new Error(historyPayload?.message || `HTTP ${historyRes.status}`);
      }
      const runs = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
      const latest = runs.length ? runs[0] : null;
      const html = `
        <div class="ontology-insight-card">
          <div class="ontology-insight-row"><span class="k">Report</span><span>${deps.escapeHtml(reportingForm)}</span></div>
          <div class="ontology-insight-row"><span class="k">Full Name</span><span>${deps.escapeHtml(metadataPayload?.fullName || '-')}</span></div>
          <div class="ontology-insight-row"><span class="k">Runs</span><span>${deps.escapeHtml(deps.formatCount(runs.length))}</span></div>
          <div class="ontology-insight-row"><span class="k">Latest Run ID</span><span>${deps.escapeHtml(latest?.runId || '-')}</span></div>
          <div class="ontology-insight-row"><span class="k">Active MDRMs</span><span>${deps.escapeHtml(deps.formatCount(latest?.activeMdrms || 0))}</span></div>
          <div class="ontology-insight-row"><span class="k">Inactive MDRMs</span><span>${deps.escapeHtml(deps.formatCount(latest?.inactiveMdrms || 0))}</span></div>
          <div class="small text-muted mt-1">${deps.escapeHtml((metadataPayload?.description || '').slice(0, 650) || 'No description available.')}</div>
          <div class="mt-2">
            <a class="btn btn-sm btn-outline-primary" href="${deps.escapeHtml(deps.buildReportDeepLink(reportingForm))}">View Full Details</a>
          </div>
        </div>
      `;
      openOntologyInsightShell(`Report: ${reportingForm}`, html);
    }

    async function loadOntologyMdrmInsight(mdrmCode) {
      const encoded = encodeURIComponent(mdrmCode);
      const response = await fetch(deps.appendRunContext(`/api/mdrm/profile?mdrm=${encoded}`));
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload?.message || `HTTP ${response.status}`);
      }
      const timeline = Array.isArray(payload?.timeline) ? payload.timeline : [];
      const previewTimeline = timeline.slice(0, 4).map(entry => `
        <div class="ontology-insight-row">
          <span class="k">Run ${deps.escapeHtml(entry?.runId || '-')}</span>
          <span>${deps.escapeHtml(entry?.status || '-')}${entry?.reportingForms ? ` | ${deps.escapeHtml(entry.reportingForms)}` : ''}</span>
        </div>
      `).join('');
      const html = `
        <div class="ontology-insight-card">
          <div class="ontology-insight-row"><span class="k">MDRM</span><span class="mono">${deps.escapeHtml(payload?.mdrmCode || mdrmCode)}</span></div>
          <div class="ontology-insight-row"><span class="k">Status</span><span>${deps.escapeHtml(payload?.status || '-')}</span></div>
          <div class="ontology-insight-row"><span class="k">Type</span><span>${deps.escapeHtml(payload?.itemType || '-')}</span></div>
          <div class="ontology-insight-row"><span class="k">Associations</span><span>${deps.escapeHtml(deps.formatCount(payload?.associationsCount || 0))}</span></div>
          <div class="ontology-insight-row"><span class="k">Related Reports</span><span>${deps.escapeHtml(deps.formatCount(payload?.relatedReportsCount || 0))}</span></div>
          <div class="small text-muted mt-1">${deps.escapeHtml(payload?.description || payload?.definition || 'No description available.')}</div>
          <div class="mt-2">
            <a class="btn btn-sm btn-outline-primary" href="${deps.escapeHtml(deps.buildMdrmProfileUrl(mdrmCode))}">View Full Details</a>
          </div>
        </div>
        <div class="ontology-insight-card">
          <div class="small fw-semibold mb-1">Recent Timeline</div>
          ${previewTimeline || '<div class="small text-muted">No timeline entries.</div>'}
        </div>
      `;
      openOntologyInsightShell(`MDRM: ${mdrmCode}`, html);
    }

    async function openOntologyInsight(type, value) {
      const normalizedType = String(type || '').trim().toLowerCase();
      const rawValue = String(value || '').trim();
      if (!normalizedType || !rawValue) {
        return;
      }
      openOntologyInsightShell('Loading...', '<div class="small text-muted">Loading details...</div>');
      try {
        if (normalizedType === 'report') {
          await loadOntologyReportInsight(rawValue);
          return;
        }
        if (normalizedType === 'mdrm') {
          await loadOntologyMdrmInsight(rawValue.toUpperCase());
          return;
        }
        if (normalizedType === 'rule') {
          const response = await fetch(deps.getRuleDetailEndpoint(rawValue));
          const payload = await response.json().catch(() => ({}));
          if (!response.ok) {
            throw new Error(payload?.message || `HTTP ${response.status}`);
          }
          const dependencies = Array.isArray(payload?.dependencies) ? payload.dependencies : [];
          const html = `
            <div class="ontology-insight-card">
              <div class="ontology-insight-row"><span class="k">Rule</span><span>${deps.escapeHtml(payload?.rule?.ruleNumber || rawValue)}</span></div>
              <div class="ontology-insight-row"><span class="k">Primary MDRM</span><span class="mono">${deps.escapeHtml(payload?.rule?.primaryMdrmCode || '-')}</span></div>
              <div class="ontology-insight-row"><span class="k">Schedule</span><span>${deps.escapeHtml(payload?.rule?.scheduleName || '-')}</span></div>
              <div class="ontology-insight-row"><span class="k">Status</span><span>${deps.escapeHtml(payload?.rule?.lineageStatus || '-')}</span></div>
              <div class="small text-muted mt-1">${deps.escapeHtml(payload?.rule?.ruleText || payload?.rule?.ruleExpression || 'No rule text available.')}</div>
            </div>
            <div class="ontology-insight-card">
              <div class="small fw-semibold mb-1">Dependencies (${dependencies.length})</div>
              ${dependencies.length ? dependencies.slice(0, 16).map(dep => `
                <div class="ontology-insight-row">
                  <span class="k mono">${deps.escapeHtml(dep.secondaryMdrmCode || dep.secondaryTokenRaw || '-')}</span>
                  <span>${deps.escapeHtml(dep.dependencyStatus || dep.secondaryMdrmStatus || '-')}</span>
                </div>
              `).join('') : '<div class="small text-muted">No dependencies found.</div>'}
            </div>
          `;
          openOntologyInsightShell(`Rule: ${deps.escapeHtml(payload?.rule?.ruleNumber || rawValue)}`, html);
          return;
        }
      } catch (error) {
        openOntologyInsightShell(
          'Details',
          `<div class="ontology-insight-card"><div class="small text-danger">Unable to load details: ${deps.escapeHtml(error.message || error)}</div></div>`
        );
      }
    }

    function buildOntologyRuleSummaryUrl(reportingForm, mdrmCode) {
      const params = new URLSearchParams();
      const normalizedReport = String(reportingForm || '').trim();
      const normalizedMdrm = String(mdrmCode || '').trim().toUpperCase();
      if (normalizedMdrm) {
        params.set('mdrm', normalizedMdrm);
        return buildOntologyUrl(`/api/mdrm/rules/by-mdrm?${params.toString()}`, {
          includeReportFilters: false,
          includeMdrmFilters: false
        });
      }
      if (!normalizedReport) {
        return '';
      }
      params.set('reportingForm', normalizedReport);
      return buildOntologyUrl(`/api/mdrm/rules/by-report?${params.toString()}`, {
        includeReportFilters: false,
        includeMdrmFilters: false
      });
    }

    function buildOntologyRuleScopeUrl(reportingForms, mdrmCodes) {
      const params = new URLSearchParams();
      const normalizedReports = Array.isArray(reportingForms)
        ? reportingForms.map(value => String(value || '').trim()).filter(Boolean)
        : [];
      const normalizedMdrms = Array.isArray(mdrmCodes)
        ? mdrmCodes.map(value => String(value || '').trim().toUpperCase()).filter(Boolean)
        : [];
      normalizedReports.forEach(reportingForm => params.append('reportingForm', reportingForm));
      normalizedMdrms.forEach(mdrmCode => params.append('mdrm', mdrmCode));
      return buildOntologyUrl(`/api/mdrm/rules/by-scope?${params.toString()}`, {
        includeReportFilters: false,
        includeMdrmFilters: false
      });
    }

    function normalizeOntologyReportKey(value) {
      return String(value || '').trim().replace(/[^A-Za-z0-9]+/g, '').toUpperCase();
    }

    function isValidOntologyMdrmCode(value) {
      return /^[A-Z]{4}[A-Z0-9]{4}$/.test(String(value || '').trim().toUpperCase());
    }

    function combineOntologyRuleSummaryPayloads(scopeType, scopeValue, payloads) {
      const uniqueRuleMap = new Map();
      (Array.isArray(payloads) ? payloads : []).forEach(payload => {
        const rules = Array.isArray(payload?.rules) ? payload.rules : [];
        rules.forEach(rule => {
          const ruleId = Number(rule?.ruleId || 0);
          if (!ruleId) {
            return;
          }
          const existing = uniqueRuleMap.get(ruleId);
          if (!existing) {
            uniqueRuleMap.set(ruleId, rule);
            return;
          }
          uniqueRuleMap.set(ruleId, {
            ...existing,
            dependencyCount: Math.max(Number(existing?.dependencyCount || 0), Number(rule?.dependencyCount || 0)),
            discrepancyCount: Math.max(Number(existing?.discrepancyCount || 0), Number(rule?.discrepancyCount || 0)),
            lineageStatus: existing?.lineageStatus && existing.lineageStatus !== 'VALID'
              ? existing.lineageStatus
              : rule?.lineageStatus
          });
        });
      });
      const rules = Array.from(uniqueRuleMap.values()).sort((left, right) => {
        const leftSchedule = String(left?.scheduleName || '');
        const rightSchedule = String(right?.scheduleName || '');
        const scheduleCompare = leftSchedule.localeCompare(rightSchedule);
        if (scheduleCompare !== 0) {
          return scheduleCompare;
        }
        return String(left?.ruleNumber || '').localeCompare(String(right?.ruleNumber || ''));
      });
      return {
        scopeType,
        scopeValue,
        runId: payloads?.[0]?.runId || null,
        asOfDate: payloads?.[0]?.asOfDate || null,
        totalRules: rules.length,
        totalDependencies: rules.reduce((sum, rule) => sum + Number(rule?.dependencyCount || 0), 0),
        discrepancyCount: rules.filter(rule => String(rule?.lineageStatus || '').toUpperCase() !== 'VALID').length,
        rules
      };
    }

    function normalizeOntologyRuleSummaryPayload(scopeType, scopeValue, payload) {
      const rules = (Array.isArray(payload?.rules) ? payload.rules : [])
        .slice()
        .sort((left, right) => {
          const leftSchedule = String(left?.scheduleName || '');
          const rightSchedule = String(right?.scheduleName || '');
          const scheduleCompare = leftSchedule.localeCompare(rightSchedule);
          if (scheduleCompare !== 0) {
            return scheduleCompare;
          }
          return String(left?.ruleNumber || '').localeCompare(String(right?.ruleNumber || ''));
        });
      return {
        scopeType,
        scopeValue,
        runId: payload?.runId || null,
        asOfDate: payload?.asOfDate || null,
        totalRules: Number(payload?.totalRules || rules.length),
        totalDependencies: Number(payload?.totalDependencies || rules.reduce((sum, rule) => sum + Number(rule?.dependencyCount || 0), 0)),
        discrepancyCount: Number(payload?.discrepancyCount || rules.filter(rule => String(rule?.lineageStatus || '').toUpperCase() !== 'VALID').length),
        rules
      };
    }

    function filterOntologyRuleSummaryPayload(payload, selectedReportKeys, selectedMdrmCodes) {
      if (!payload) {
        return null;
      }
      const reportKeySet = new Set((Array.isArray(selectedReportKeys) ? selectedReportKeys : []).filter(Boolean));
      const mdrmSet = new Set((Array.isArray(selectedMdrmCodes) ? selectedMdrmCodes : []).filter(Boolean));
      const rules = (Array.isArray(payload.rules) ? payload.rules : []).filter(rule => {
        const reportMatches = !reportKeySet.size
          || reportKeySet.has(normalizeOntologyReportKey(rule?.reportingForm || rule?.reportSeries || ''));
        const mdrmMatches = !mdrmSet.size
          || mdrmSet.has(String(rule?.primaryMdrmCode || '').trim().toUpperCase());
        return reportMatches && mdrmMatches;
      });
      return {
        ...payload,
        totalRules: rules.length,
        totalDependencies: rules.reduce((sum, rule) => sum + Number(rule?.dependencyCount || 0), 0),
        discrepancyCount: rules.filter(rule => String(rule?.lineageStatus || '').toUpperCase() !== 'VALID').length,
        rules
      };
    }

    async function loadOntologyRuleSummaryPayload(selectedReports, selectedMdrms) {
      const explicitReports = Array.isArray(selectedReports) && selectedReports.length ? selectedReports : [];
      const explicitMdrms = Array.isArray(selectedMdrms) && selectedMdrms.length ? selectedMdrms : [];
      const normalizedReports = explicitReports.length
        ? explicitReports.map(value => String(value || '').trim()).filter(Boolean)
        : getEffectiveSelectedReports();
      const normalizedReportKeys = normalizedReports
        .map(value => normalizeOntologyReportKey(value))
        .filter(Boolean);
      const normalizedMdrms = explicitMdrms.length
        ? explicitMdrms.map(value => String(value || '').trim().toUpperCase()).filter(value => isValidOntologyMdrmCode(value))
        : getEffectiveSelectedMdrms();

      if (!normalizedReports.length && !normalizedMdrms.length) {
        setOntologyRuleSummaryPayload(null);
        renderOntologyScopeChips();
        renderOntologyRulesTab(null);
        return null;
      }
      const response = await fetch(buildOntologyRuleScopeUrl(normalizedReports, normalizedMdrms));
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload?.message || `HTTP ${response.status}`);
      }
      const normalizedPayload = normalizeOntologyRuleSummaryPayload(
        normalizedMdrms.length === 1
          ? 'MDRM'
          : normalizedReports.length === 1
            ? 'REPORT'
            : normalizedReports.length > 1
              ? 'REPORT_SET'
              : 'MDRM_SET',
        normalizedMdrms.length === 1
          ? normalizedMdrms[0]
          : normalizedReports.length === 1
            ? normalizedReports[0]
            : normalizedReports.length > 1
              ? `${deps.formatCount(normalizedReports.length)} Reports`
              : `${deps.formatCount(normalizedMdrms.length)} MDRMs`,
        payload
      );
      const filteredPayload = filterOntologyRuleSummaryPayload(normalizedPayload, normalizedReportKeys, normalizedMdrms);
      setOntologyRuleSummaryPayload(filteredPayload);
      renderOntologyScopeChips();
      renderOntologyRulesTab(filteredPayload);
      return filteredPayload;
    }

    async function openOntologyRuleSummaryInsight() {
      let payload = null;
      let selectedReports = [];
      let selectedMdrms = [];
      try {
        selectedReports = getEffectiveSelectedReports();
        selectedMdrms = getEffectiveSelectedMdrms();
        const response = await fetch(buildOntologyRuleScopeUrl(selectedReports, selectedMdrms));
        payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
      } catch (error) {
        if (el.ontologyRulesMeta) {
          el.ontologyRulesMeta.textContent = 'Unable to load associated rules';
        }
        if (el.ontologyRulesContainer) {
          el.ontologyRulesContainer.innerHTML = `<div class="text-danger small p-3">Unable to load associated rules: ${deps.escapeHtml(error.message || error)}</div>`;
        }
        setActiveOntologyInspectorTab('rules');
        return;
      }
      const normalizedPayload = normalizeOntologyRuleSummaryPayload(
        selectedMdrms.length === 1
          ? 'MDRM'
          : selectedReports.length === 1
            ? 'REPORT'
            : 'SCOPE',
        selectedMdrms.length === 1
          ? selectedMdrms[0]
          : selectedReports.length === 1
            ? selectedReports[0]
            : 'Current ontology scope',
        payload
      );
      const filteredPayload = filterOntologyRuleSummaryPayload(
        normalizedPayload,
        selectedReports.map(value => normalizeOntologyReportKey(value)).filter(Boolean),
        selectedMdrms.map(value => String(value || '').trim().toUpperCase()).filter(Boolean)
      );
      payload = filteredPayload || payload;
      setOntologyRuleSummaryPayload(payload || null);
      renderOntologyScopeChips();
      renderOntologyRulesTab(payload || null);
      setActiveOntologyInspectorTab('rules');
    }

    async function syncOntologyRuleScope({ reports = null, mdrms = null } = {}) {
      const scopedReports = Array.isArray(reports) ? reports : getEffectiveSelectedReports();
      const scopedMdrms = Array.isArray(mdrms) ? mdrms : getEffectiveSelectedMdrms();
      try {
        await loadOntologyRuleSummaryPayload(scopedReports, scopedMdrms);
        renderOntologyDetail(getOntologySelectedNodeId() ? getOntologyNodeById().get(getOntologySelectedNodeId()) || null : null);
        renderOntologyReferenceTables();
      } catch (error) {
        setOntologyRuleSummaryPayload(null);
        renderOntologyScopeChips();
        renderOntologyRulesTab(null);
        if (el.ontologyRulesMeta) {
          el.ontologyRulesMeta.textContent = `Unable to load scoped rules: ${error.message || error}`;
        }
        renderOntologyDetail(getOntologySelectedNodeId() ? getOntologyNodeById().get(getOntologySelectedNodeId()) || null : null);
      }
    }

    function selectedOntologyReportForManage() {
      if (getOntologyFocusedReport()) {
        return String(getOntologyFocusedReport()).trim();
      }
      const selectedReports = (getOntologySelectedReportsState() || []).map(v => String(v).trim()).filter(Boolean);
      if (selectedReports.length === 1) {
        return selectedReports[0];
      }
      return '';
    }

    function selectedOntologyMdrmForManage() {
      if (getOntologyFocusedMdrm()) {
        return String(getOntologyFocusedMdrm()).trim().toUpperCase();
      }
      const selectedMdrms = (getOntologySelectedMdrmsState() || []).map(v => String(v).trim()).filter(Boolean);
      if (selectedMdrms.length === 1) {
        return selectedMdrms[0].toUpperCase();
      }
      return '';
    }

    function isLatestRunSelectedForManage() {
      const selected = Number(deps.getRunContextId() || 0);
      const latest = Number(deps.getLatestAvailableRunId() || 0);
      return selected > 0 && latest > 0 && selected === latest;
    }

    function syncOntologyManageActions() {
      // Node edit affordance is always visible; validation is enforced when modal opens/submits.
    }

    function syncOntologyManageModalFields() {
      if (!el.ontologyManageOperation) return;
      const op = String(el.ontologyManageOperation.value || 'ADD').toUpperCase();
      const isAdd = op === 'ADD';
      const isEdit = op === 'EDIT';
      if (el.ontologyManageMdrmAddWrap) {
        el.ontologyManageMdrmAddWrap.style.display = isAdd ? '' : 'none';
      }
      if (el.ontologyManageMdrmSelectWrap) {
        el.ontologyManageMdrmSelectWrap.style.display = isAdd ? 'none' : '';
      }
      if (el.ontologyManageDescriptionWrap) {
        el.ontologyManageDescriptionWrap.style.display = isEdit || isAdd ? '' : 'none';
      }
      if (el.ontologyManageModalHint) {
        el.ontologyManageModalHint.textContent = isAdd
          ? 'Add a new MDRM for selected report.'
          : isEdit
            ? 'Update description for selected MDRM.'
            : 'Delete performs soft delete (marks inactive).';
      }
    }

    async function loadOntologyManageMdrmOptions() {
      const report = String(el.ontologyManageReportInput?.value || '').trim();
      setOntologyManageMdrmOptionList([]);
      if (!el.ontologyManageMdrmOptions) {
        return;
      }
      el.ontologyManageMdrmOptions.innerHTML = '';
      const selectedScopedMdrms = (getOntologySelectedMdrmsState() || [])
        .map(value => String(value || '').trim().toUpperCase())
        .filter(Boolean);
      if (selectedScopedMdrms.length > 0) {
        const deduped = Array.from(new Set(selectedScopedMdrms)).sort((a, b) => a.localeCompare(b));
        setOntologyManageMdrmOptionList(deduped);
        el.ontologyManageMdrmOptions.innerHTML = deduped
          .map(code => `<option value="${deps.escapeHtml(code)}"></option>`)
          .join('');
        return;
      }
      try {
        const params = new URLSearchParams();
        const runId = deps.getRunContextId();
        if (runId) {
          params.set('runId', String(runId));
        }
        if (report) {
          params.append('reportingForm', report);
        } else {
          (getOntologySelectedReportsState() || []).forEach(value => {
            const next = String(value || '').trim();
            if (next) params.append('reportingForm', next);
          });
        }
        const query = params.toString();
        const url = query ? `/api/mdrm/ontology-options?${query}` : '/api/mdrm/ontology-options';
        const response = await fetch(url);
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
        const mdrms = Array.isArray(payload?.mdrms) ? payload.mdrms : [];
        const normalized = mdrms.map(v => String(v || '').trim().toUpperCase()).filter(Boolean);
        setOntologyManageMdrmOptionList(normalized);
        el.ontologyManageMdrmOptions.innerHTML = normalized
          .map(code => `<option value="${deps.escapeHtml(code)}"></option>`)
          .join('');
      } catch (_) {
        setOntologyManageMdrmOptionList([]);
        el.ontologyManageMdrmOptions.innerHTML = '';
      }
    }

    function openOntologyManageModal({ operation = 'ADD', report = '', mdrm = '' } = {}) {
      if (!deps.isAdminUser()) {
        setOntologyStatus('Admin access required for MDRM add/edit/delete.', 'err');
        return;
      }
      if (!isLatestRunSelectedForManage()) {
        setOntologyStatus('Select latest run context before managing MDRM.', 'err');
        return;
      }
      if (!getOntologyManageModal() && el.ontologyManageModalEl && window.bootstrap) {
        setOntologyManageModal(new bootstrap.Modal(el.ontologyManageModalEl));
      }
      if (!getOntologyManageModal() || !el.ontologyManageOperation) {
        setOntologyStatus('Manage modal is unavailable.', 'err');
        return;
      }
      const chosenReport = String(report || selectedOntologyReportForManage() || '').trim();
      const chosenMdrm = String(mdrm || selectedOntologyMdrmForManage() || '').trim().toUpperCase();
      el.ontologyManageOperation.value = String(operation || 'ADD').toUpperCase();
      if (el.ontologyManageReportInput) el.ontologyManageReportInput.value = chosenReport;
      if (el.ontologyManageMdrmAddInput) el.ontologyManageMdrmAddInput.value = chosenMdrm;
      if (el.ontologyManageMdrmSelectInput) el.ontologyManageMdrmSelectInput.value = chosenMdrm;
      if (el.ontologyManageDescriptionInput) el.ontologyManageDescriptionInput.value = '';
      syncOntologyManageModalFields();
      if (el.ontologyManageOperation.value !== 'ADD') {
        loadOntologyManageMdrmOptions().catch(() => {});
      }
      getOntologyManageModal().show();
      if (el.ontologyManageReportInput) {
        window.setTimeout(() => el.ontologyManageReportInput.focus(), 80);
      }
    }

    async function submitOntologyManageModal() {
      const operation = String(el.ontologyManageOperation?.value || 'ADD').toUpperCase();
      const report = String(el.ontologyManageReportInput?.value || '').trim();
      const mdrm = operation === 'ADD'
        ? String(el.ontologyManageMdrmAddInput?.value || '').trim().toUpperCase()
        : String(el.ontologyManageMdrmSelectInput?.value || '').trim().toUpperCase();
      const description = String(el.ontologyManageDescriptionInput?.value || '').trim();

      if (!report) {
        setOntologyStatus('Reporting form is required.', 'err');
        return;
      }
      if (!mdrm) {
        setOntologyStatus('MDRM code is required.', 'err');
        return;
      }
      if (operation !== 'ADD') {
        const allowed = new Set((getOntologyManageMdrmOptionList() || []).map(v => String(v).toUpperCase()));
        if (!allowed.has(mdrm)) {
          setOntologyStatus('Choose MDRM from dropdown options.', 'err');
          return;
        }
      }
      if (operation === 'EDIT' && !description) {
        setOntologyStatus('Description is required for edit.', 'err');
        return;
      }
      const confirm = window.confirm(
        `Confirm ${operation}?\nRun: ${deps.getRunContextId()}\nReport: ${report}\nMDRM: ${mdrm}`
      );
      if (!confirm) return;

      try {
        let endpoint = '';
        let body = null;
        if (operation === 'ADD') {
          endpoint = '/api/mdrm/manage/add';
          body = {
            runId: deps.getRunContextId(),
            reportingForm: report,
            mdrmCode: mdrm,
            description: description || null
          };
        } else if (operation === 'EDIT') {
          endpoint = '/api/mdrm/manage/edit';
          body = {
            runId: deps.getRunContextId(),
            reportingForm: report,
            mdrmCode: mdrm,
            newMdrmCode: mdrm,
            description
          };
        } else {
          endpoint = '/api/mdrm/manage/delete';
          body = {
            runId: deps.getRunContextId(),
            reportingForm: report,
            mdrmCode: mdrm
          };
        }
        const response = await deps.apiFetch(endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
        setOntologyFocusedReport(report);
        setOntologyFocusedMdrm(mdrm);
        if (getOntologyManageModal()) {
          getOntologyManageModal().hide();
        }
        setOntologyStatus(payload?.message || 'MDRM operation completed.', 'ok');
        await deps.refreshForms();
        await loadOntologyGraph(true);
      } catch (error) {
        setOntologyStatus(`${operation} failed: ${error.message || error}`, 'err');
      }
    }

    function removeOntologyInfoNote() {
      if (getOntologyInfoNoteEl() && getOntologyInfoNoteEl().parentNode) {
        getOntologyInfoNoteEl().parentNode.removeChild(getOntologyInfoNoteEl());
      }
      setOntologyInfoNoteEl(null);
    }

    function clearOntologyGraph(message = 'Select one or more reports to build ontology graph.') {
      removeOntologyInfoNote();
      if (getOntologyGraphSimulation()) {
        getOntologyGraphSimulation().stop();
        setOntologyGraphSimulation(null);
      }
      if (el.ontologyGraphHost) {
        el.ontologyGraphHost.innerHTML = `<div class="text-muted small p-3">${deps.escapeHtml(message)}</div>`;
      }
      setOntologyGraphPayload(null);
      setOntologyGraphNodeSelection(null);
      setOntologyGraphLinkSelection(null);
      setOntologyNeighborMap(new Map());
      setOntologyNodeById(new Map());
      renderOntologyDetail(null);
      syncOntologyManageActions();
    }

    function toggleOntologyInfoNote(message, anchorX, anchorY, hostWidth, hostHeight) {
      if (!el.ontologyGraphHost) {
        return;
      }
      if (getOntologyInfoNoteEl()) {
        removeOntologyInfoNote();
        return;
      }
      const note = document.createElement('div');
      note.className = 'ontology-info-note';
      note.textContent = String(message || '').trim();
      el.ontologyGraphHost.appendChild(note);
      const noteRect = note.getBoundingClientRect();
      const left = Math.max(8, Math.min((hostWidth || 900) - noteRect.width - 8, anchorX + 14));
      const top = Math.max(8, Math.min((hostHeight || 500) - noteRect.height - 8, anchorY - 10));
      note.style.left = `${left}px`;
      note.style.top = `${top}px`;
      setOntologyInfoNoteEl(note);
    }

    function splitOntologyNodeLabel(label) {
      const raw = String(label || '').trim();
      const match = raw.match(/^(.*)\s\(([^()]*)\)$/);
      if (!match) {
        return { title: raw, count: '' };
      }
      return {
        title: String(match[1] || '').trim(),
        count: `(${String(match[2] || '').trim()})`
      };
    }

    function wrapOntologyText(text, maxCharsPerLine, maxLines) {
      const words = String(text || '').split(/\s+/).filter(Boolean);
      if (!words.length) {
        return [];
      }
      const lines = [];
      let current = '';
      words.forEach(word => {
        const next = current ? `${current} ${word}` : word;
        if (next.length <= maxCharsPerLine || !current) {
          current = next;
        } else {
          lines.push(current);
          current = word;
        }
      });
      if (current) {
        lines.push(current);
      }
      if (lines.length <= maxLines) {
        return lines;
      }
      const clipped = lines.slice(0, maxLines);
      const lastIndex = clipped.length - 1;
      clipped[lastIndex] = `${clipped[lastIndex].slice(0, Math.max(1, maxCharsPerLine - 1)).trimEnd()}…`;
      return clipped;
    }

    function appendOntologyNodeLabels(selection) {
      selection.each(function (d) {
        const group = d3.select(this);
        const radius = (function () {
          if (d.id === 'summary:report' || d.id === 'summary:mdrm') return 42;
          if (d.id === 'summary:rules') return 40;
          return 32;
        })();
        const parts = splitOntologyNodeLabel(d.label || '');
        const maxChars = radius >= 42 ? 14 : radius >= 40 ? 12 : 11;
        const titleLines = wrapOntologyText(parts.title, maxChars, parts.count ? 2 : 3);
        const lines = parts.count ? [...titleLines, parts.count] : titleLines;
        const text = group.append('text')
          .attr('class', 'ontology-node-label')
          .attr('text-anchor', 'middle')
          .attr('y', 0);

        const dynamicFontPx = radius >= 42 ? 10 : radius >= 40 ? 9.4 : 8.5;
        const lineHeight = dynamicFontPx + 1.6;
        const firstDy = -((Math.max(1, lines.length) - 1) * lineHeight) / 2;
        text.style('font-size', `${dynamicFontPx}px`);

        lines.forEach((line, index) => {
          text.append('tspan')
            .attr('x', 0)
            .attr('dy', index === 0 ? `${firstDy}px` : `${lineHeight}px`)
            .text(line);
        });
      });
    }

    function renderOntologySummaryGraph(payload) {
      if (!el.ontologyGraphHost) return;
      removeOntologyInfoNote();
      if (!window.d3) {
        setOntologyStatus('D3 library is unavailable.', 'err');
        return;
      }
      if (getOntologyGraphSimulation()) {
        getOntologyGraphSimulation().stop();
        setOntologyGraphSimulation(null);
      }
      el.ontologyGraphHost.innerHTML = '';
      setOntologyNeighborMap(new Map());
      setOntologyNodeById(new Map());
      setOntologyGraphNodeSelection(null);
      setOntologyGraphLinkSelection(null);
      setOntologyGraphLinkLabelSelection(null);
      setOntologySelectedNodeId(null);

      const reportCount = Number(payload?.reportCount || 0);
      const mdrmCount = Number(payload?.activeMdrmCount || payload?.mdrmCount || 0);
      const ruleSummaryPayload = getOntologyRuleSummaryPayload();
      const ruleCount = Number(payload?.ruleCount || 0);
      const ruleDiscrepancyCount = Number(payload?.ruleDiscrepancyCount || 0);
      const showRuleSummaryNode = ruleSummaryPayload !== null || ruleCount > 0;

      const nodes = [];
      const links = [];
      if (reportCount > 0) {
        nodes.push({ id: 'summary:report', label: `Reports (${deps.formatCount(reportCount)})`, category: 'REPORT' });
      }
      if (mdrmCount > 0) {
        nodes.push({ id: 'summary:mdrm', label: `Active MDRMs (${deps.formatCount(mdrmCount)})`, category: 'MDRM', status: 'Active' });
      }
      if (showRuleSummaryNode) {
        nodes.push({
          id: 'summary:rules',
          label: `Rules (${deps.formatCount(ruleCount)})`,
          category: 'RULE',
          status: ruleDiscrepancyCount > 0 ? 'Discrepancy' : 'Valid'
        });
      }

      const nodeIdSet = new Set(nodes.map(node => node.id));
      if (nodeIdSet.has('summary:report') && nodeIdSet.has('summary:mdrm')) {
        links.push({ source: 'summary:report', target: 'summary:mdrm', relation: 'REPORT_HAS_MDRM' });
      }
      if (nodeIdSet.has('summary:mdrm') && nodeIdSet.has('summary:rules')) {
        links.push({ source: 'summary:mdrm', target: 'summary:rules', relation: 'MDRM_HAS_RULE_SUMMARY' });
      }

      if (!nodes.length) {
        el.ontologyGraphHost.innerHTML = '<div class="text-muted small p-3">No nodes to display for current selection.</div>';
        return;
      }

      const width = Math.max(880, el.ontologyGraphHost.clientWidth || 880);
      const paneHeight = Math.floor(el.ontologyGraphHost.clientHeight || 0);
      const height = Math.max(380, paneHeight || 0);
      const centerY = Math.round(height / 2);
      const x1 = Math.round(width * 0.22);
      const x2 = Math.round(width * 0.5);
      const x3 = Math.round(width * 0.78);
      const positions = new Map();
      positions.set('summary:report', { x: x1, y: centerY });
      positions.set('summary:mdrm', { x: x2, y: centerY });
      if (showRuleSummaryNode) {
        positions.set('summary:rules', { x: x3, y: centerY });
      }

      nodes.forEach(node => {
        const pos = positions.get(node.id);
        if (!pos) {
          return;
        }
        node.x = pos.x;
        node.y = pos.y;
        getOntologyNodeById().set(node.id, node);
      });
      links.forEach(link => {
        if (!getOntologyNeighborMap().has(link.source)) getOntologyNeighborMap().set(link.source, new Set());
        if (!getOntologyNeighborMap().has(link.target)) getOntologyNeighborMap().set(link.target, new Set());
        getOntologyNeighborMap().get(link.source).add(link.target);
        getOntologyNeighborMap().get(link.target).add(link.source);
      });

      const svg = d3.select(el.ontologyGraphHost)
        .append('svg')
        .attr('class', 'ontology-graph')
        .attr('width', width)
        .attr('height', height)
        .attr('viewBox', `0 0 ${width} ${height}`)
        .attr('role', 'img')
        .attr('aria-label', 'Summary ontology graph');

      setOntologyGraphSvg(svg);
      setOntologyGraphLinkSelection(svg.append('g')
        .attr('class', 'ontology-links')
        .selectAll('line')
        .data(links)
        .join('line')
        .attr('class', 'ontology-link summary-flow')
        .attr('x1', d => positions.get(d.source).x)
        .attr('y1', d => positions.get(d.source).y)
        .attr('x2', d => positions.get(d.target).x)
        .attr('y2', d => positions.get(d.target).y));

      setOntologyGraphLinkLabelSelection(svg.append('g')
        .attr('class', 'ontology-link-labels')
        .selectAll('text')
        .data(links)
        .join('text')
        .attr('class', 'ontology-link-label')
        .attr('text-anchor', 'middle')
        .attr('x', d => (positions.get(d.source).x + positions.get(d.target).x) / 2)
        .attr('y', d => ((positions.get(d.source).y + positions.get(d.target).y) / 2) - 8)
        .text(d => {
          if (d.relation === 'REPORT_HAS_MDRM') return 'comprises of';
          if (d.relation === 'MDRM_HAS_RULE_SUMMARY') return 'governed by';
          return 'can be of';
        }));

      setOntologyGraphNodeSelection(svg.append('g')
        .attr('class', 'ontology-nodes')
        .selectAll('g')
        .data(nodes)
        .join('g')
        .attr('class', d => `${ontologyNodeClass(d)} ontology-summary-node`)
        .attr('transform', d => `translate(${d.x}, ${d.y})`)
        .on('click', (_, d) => {
          if (d.id === 'summary:report') {
            openOntologySelector('report');
            return;
          }
          if (d.id === 'summary:mdrm') {
            openOntologySelector('mdrm');
            return;
          }
          if (d.id === 'summary:rules') {
            openOntologyRuleSummaryInsight().catch(() => {});
            return;
          }
        }));

      getOntologyGraphNodeSelection().append('circle')
        .attr('r', d => {
          if (d.id === 'summary:report') return 42;
          if (d.id === 'summary:mdrm') return 42;
          if (d.id === 'summary:rules') return 40;
          return 32;
        });

      appendOntologyNodeLabels(getOntologyGraphNodeSelection());

      getOntologyGraphNodeSelection().append('title')
        .text(d => d.label);

      const mdrmInfoIcon = getOntologyGraphNodeSelection()
        .filter(d => d.id === 'summary:mdrm')
        .append('g')
        .attr('class', 'ontology-node-info')
        .attr('transform', 'translate(27,-27)')
        .on('click', (event, d) => {
          event.stopPropagation();
          toggleOntologyInfoNote(
            'Each data series is given a four-letter mnemonic, which is used for data transmission and storage. Each variable, within a data series, is assigned a number (usually 4 digits).Combining the series mnemonic and number references a specific data item on a specific series and is known as the MDRM number',
            (d.x || 0) + 27,
            (d.y || 0) - 27,
            width,
            height
          );
        });

      mdrmInfoIcon.append('circle').attr('r', 9);
      mdrmInfoIcon.append('text')
        .attr('text-anchor', 'middle')
        .attr('dy', 3.4)
        .text('i');

      if (deps.isAdminUser()) {
        const manageIcon = getOntologyGraphNodeSelection()
          .filter(d => normalizeOntologyCategory(d.category) === 'MDRM')
          .append('g')
          .attr('class', 'ontology-node-info ontology-node-edit')
          .attr('transform', 'translate(-27,-27)')
          .on('click', (event, d) => {
            event.stopPropagation();
            const selectedReport = selectedOntologyReportForManage();
            const selectedMdrm = getOntologySelectedMdrmsState().includes(String(d.label || '').trim().toUpperCase())
              ? String(d.label || '').trim().toUpperCase()
              : selectedOntologyMdrmForManage();
            const hasExactMdrm = !!selectedMdrm;
            openOntologyManageModal({
              operation: hasExactMdrm ? 'EDIT' : 'ADD',
              report: selectedReport,
              mdrm: hasExactMdrm ? selectedMdrm : ''
            });
          });

        manageIcon.append('circle').attr('r', 8);
        manageIcon.append('text')
          .attr('text-anchor', 'middle')
          .attr('dy', 3.2)
          .text('✎');
      }

      refreshOntologyHighlight();
    }

    function renderOntologyDetail(node) {
      if (!el.ontologyDetailPanel) return;
      const selectionSentence = buildOntologySelectionSentence(node);
      if (!node) {
        el.ontologyDetailPanel.innerHTML = `
          <div class="ontology-detail-card">
            <div class="ontology-detail-title">Current Selection</div>
            <div class="text-muted small">${deps.escapeHtml(selectionSentence)}</div>
          </div>
        `;
        return;
      }
      const neighborIds = Array.from(getOntologyNeighborMap().get(node.id) || []);
      const neighbors = neighborIds.map(id => getOntologyNodeById().get(id)).filter(Boolean);
      const category = normalizeOntologyCategory(node.category);
      if (category === 'REPORT') {
        const mdrms = neighbors.filter(item => normalizeOntologyCategory(item.category) === 'MDRM');
        el.ontologyDetailPanel.innerHTML = `
          <div class="ontology-detail-card">
            <div class="ontology-detail-title">Report Node</div>
            <div class="text-muted small mb-2">${deps.escapeHtml(selectionSentence)}</div>
            <div class="ontology-detail-row"><span class="k">Report</span><span>${deps.escapeHtml(node.label || '-')}</span></div>
            <div class="ontology-detail-row"><span class="k">Connected MDRMs</span><span>${deps.escapeHtml(mdrms.length)}</span></div>
            <button id="ontologyOpenReportBtn" class="btn btn-sm btn-outline-primary mt-2" type="button">Open report view</button>
          </div>
        `;
        document.getElementById('ontologyOpenReportBtn')?.addEventListener('click', () => deps.openReportFromPalette(node.label || ''));
        return;
      }
      if (category === 'MDRM') {
        const reports = neighbors.filter(item => normalizeOntologyCategory(item.category) === 'REPORT');
        el.ontologyDetailPanel.innerHTML = `
          <div class="ontology-detail-card">
            <div class="ontology-detail-title">MDRM Node</div>
            <div class="text-muted small mb-2">${deps.escapeHtml(selectionSentence)}</div>
            <div class="ontology-detail-row"><span class="k">MDRM</span><span>${deps.escapeHtml(node.label || '-')}</span></div>
            <div class="ontology-detail-row"><span class="k">Status</span><span>${deps.statusCapsule(node.status || '-')}</span></div>
            <div class="ontology-detail-row"><span class="k">Reports</span><span>${deps.escapeHtml(reports.length)}</span></div>
            <a class="btn btn-sm btn-outline-primary mt-2" href="${deps.escapeHtml(deps.buildMdrmProfileUrl(node.label || ''))}">Open MDRM profile</a>
            <button id="ontologyRefineMdrmBtn" class="btn btn-sm btn-outline-secondary mt-2" type="button">Refine MDRM selection</button>
          </div>
        `;
        document.getElementById('ontologyRefineMdrmBtn')?.addEventListener('click', () => openOntologySelector('mdrm'));
        return;
      }
      if (category === 'MDRM_TYPE') {
        const members = neighbors.filter(item => normalizeOntologyCategory(item.category) === 'MDRM').slice(0, 25);
        el.ontologyDetailPanel.innerHTML = `
          <div class="ontology-detail-card">
            <div class="ontology-detail-title">MDRM Type Node</div>
            <div class="text-muted small mb-2">${deps.escapeHtml(selectionSentence)}</div>
            <div class="ontology-detail-row"><span class="k">Type</span><span>${deps.escapeHtml(node.label || '-')}</span></div>
            <div class="ontology-detail-row"><span class="k">MDRMs</span><span>${deps.escapeHtml(neighbors.length)}</span></div>
            <div class="ontology-member-list">
              ${members.length ? members.map(item => `<div class="ontology-member-item">${deps.escapeHtml(item.label || '-')}</div>`).join('') : '<div class="text-muted small">No MDRMs connected.</div>'}
            </div>
          </div>
        `;
        return;
      }
      if (category === 'RULE') {
        const connectedMdrms = neighbors.filter(item => normalizeOntologyCategory(item.category) === 'MDRM');
        el.ontologyDetailPanel.innerHTML = `
          <div class="ontology-detail-card">
            <div class="ontology-detail-title">Rule Node</div>
            <div class="text-muted small mb-2">${deps.escapeHtml(selectionSentence)}</div>
            <div class="ontology-detail-row"><span class="k">Rule</span><span>${deps.escapeHtml(node.label || '-')}</span></div>
            <div class="ontology-detail-row"><span class="k">Status</span><span>${deps.escapeHtml(node.status || '-')}</span></div>
            <div class="ontology-detail-row"><span class="k">Connected MDRMs</span><span>${deps.escapeHtml(connectedMdrms.length)}</span></div>
            <button id="ontologyOpenRuleBtn" class="btn btn-sm btn-outline-primary mt-2" type="button">Open rule detail</button>
          </div>
        `;
        document.getElementById('ontologyOpenRuleBtn')?.addEventListener('click', () => {
          const rawId = String(node.id || '').split(':')[1] || '';
          openOntologyInsight('rule', rawId);
        });
        return;
      }
      el.ontologyDetailPanel.innerHTML = `
        <div class="ontology-detail-card">
          <div class="ontology-detail-title">Node</div>
          <div class="text-muted small mb-2">${deps.escapeHtml(selectionSentence)}</div>
          <div class="ontology-detail-row"><span class="k">Label</span><span>${deps.escapeHtml(node.label || '-')}</span></div>
        </div>
      `;
    }

    function refreshOntologyHighlight() {
      if (!getOntologyGraphNodeSelection() || !getOntologyGraphLinkSelection()) return;
      const filterValue = String(el.ontologyQuickFilterInput?.value || '').trim().toLowerCase();
      const selectedModes = [];
      if (el.ontologyFilterReport?.checked) selectedModes.push('report');
      if (el.ontologyFilterMdrm?.checked) selectedModes.push('mdrm');
      if (el.ontologyFilterRule?.checked) selectedModes.push('rule');
      const modeSet = new Set(selectedModes);
      const selectedId = getOntologySelectedNodeId();
      const visibleById = new Map();

      function matchesMode(node) {
        if (!modeSet.size) {
          return true;
        }
        const category = normalizeOntologyCategory(node?.category);
        const status = String(node?.status || '').toLowerCase();
        if (category === 'REPORT') return modeSet.has('report');
        if (category === 'MDRM_TYPE') return false;
        if (category === 'RULE') return modeSet.has('rule');
        if (category === 'MDRM') {
          if (status === 'inactive') return false;
          return modeSet.has('mdrm');
        }
        return false;
      }

      getOntologyGraphNodeSelection()
        .classed('is-hidden', d => {
          const matchesText = !filterValue || String(d.label || '').toLowerCase().includes(filterValue);
          const visible = matchesText && matchesMode(d);
          visibleById.set(d.id, visible);
          return !visible;
        })
        .classed('is-dim', false)
        .classed('is-selected', d => selectedId && visibleById.get(d.id) && d.id === selectedId);

      getOntologyGraphLinkSelection()
        .classed('is-hidden', d => {
          const sourceId = String(d.source?.id || d.source || '');
          const targetId = String(d.target?.id || d.target || '');
          return !(visibleById.get(sourceId) && visibleById.get(targetId));
        })
        .classed('is-dim', false);

      if (getOntologyGraphLinkLabelSelection()) {
        getOntologyGraphLinkLabelSelection().classed('is-hidden', d => {
          const sourceId = String(d.source?.id || d.source || '');
          const targetId = String(d.target?.id || d.target || '');
          return !(visibleById.get(sourceId) && visibleById.get(targetId));
        });
      }
    }

    function resetOntologyView() {
      setOntologySelectedNodeId(null);
      setActiveOntologyInspectorTab('summary');
      if (el.ontologyQuickFilterInput) el.ontologyQuickFilterInput.value = '';
      if (el.ontologyFilterReport) el.ontologyFilterReport.checked = true;
      if (el.ontologyFilterMdrm) el.ontologyFilterMdrm.checked = true;
      if (el.ontologyFilterRule) el.ontologyFilterRule.checked = true;
      refreshOntologyHighlight();
      syncOntologyManageActions();
    }

    async function clearOntologyView() {
      resetOntologyScopeState();
      resetOntologyView();
      closeOntologySelector();
      await loadOntologyGraph(true);
      setOntologyStatus('View cleared to base summary.', 'ok');
    }

    function renderOntologyGraph(payload) {
      if (!el.ontologyGraphHost) return;
      if (!window.d3) {
        setOntologyStatus('D3 library is unavailable.', 'err');
        return;
      }
      if (getOntologyGraphSimulation()) {
        getOntologyGraphSimulation().stop();
        setOntologyGraphSimulation(null);
      }
      el.ontologyGraphHost.innerHTML = '';
      setOntologyNeighborMap(new Map());
      setOntologyNodeById(new Map());
      setOntologyGraphNodeSelection(null);
      setOntologyGraphLinkSelection(null);

      const nodes = (Array.isArray(payload?.nodes) ? payload.nodes : [])
        .filter(node => {
          const category = normalizeOntologyCategory(node?.category);
          if (category === 'MDRM_TYPE') {
            return false;
          }
          if (category === 'MDRM' && String(node?.status || '').trim().toUpperCase() === 'INACTIVE') {
            return false;
          }
          return category === 'REPORT' || category === 'MDRM' || category === 'RULE';
        })
        .map(node => ({ ...node }));
      const links = (Array.isArray(payload?.edges) ? payload.edges : [])
        .map(edge => ({ source: edge.source, target: edge.target, relation: edge.relation || '', status: edge.status || '' }));
      nodes.forEach(node => getOntologyNodeById().set(node.id, node));
      const resolvedLinks = links.filter(edge => getOntologyNodeById().has(edge.source) && getOntologyNodeById().has(edge.target));

      const connectedNodeIds = new Set();
      resolvedLinks.forEach(edge => {
        connectedNodeIds.add(edge.source);
        connectedNodeIds.add(edge.target);
      });
      const visibleNodes = nodes.filter(node => connectedNodeIds.has(node.id) || normalizeOntologyCategory(node.category) === 'REPORT');
      setOntologyNodeById(new Map());
      visibleNodes.forEach(node => getOntologyNodeById().set(node.id, node));
      const visibleLinks = links.filter(edge => getOntologyNodeById().has(edge.source) && getOntologyNodeById().has(edge.target));

      visibleLinks.forEach(edge => {
        if (!getOntologyNeighborMap().has(edge.source)) getOntologyNeighborMap().set(edge.source, new Set());
        if (!getOntologyNeighborMap().has(edge.target)) getOntologyNeighborMap().set(edge.target, new Set());
        getOntologyNeighborMap().get(edge.source).add(edge.target);
        getOntologyNeighborMap().get(edge.target).add(edge.source);
      });

      if (!visibleNodes.length || !visibleLinks.length) {
        clearOntologyGraph('No graph relationships for the current selection.');
        return;
      }

      const width = Math.max(880, el.ontologyGraphHost.clientWidth || 880);
      const height = Math.max(420, Math.floor(el.ontologyGraphHost.clientHeight || 640));
      const svg = d3.select(el.ontologyGraphHost)
        .append('svg')
        .attr('class', 'ontology-graph')
        .attr('width', width)
        .attr('height', height)
        .attr('viewBox', `0 0 ${width} ${height}`)
        .attr('role', 'img')
        .attr('aria-label', 'Ontology graph of Report to MDRM to MDRM Type');

      setOntologyGraphSvg(svg);
      setOntologyGraphLinkSelection(svg.append('g')
        .attr('class', 'ontology-links')
        .selectAll('line')
        .data(visibleLinks)
        .join('line')
        .attr('class', d => `ontology-link ${String(d.relation || '').toLowerCase()}`));

      const linkLabelSelection = svg.append('g')
        .attr('class', 'ontology-link-labels')
        .selectAll('text')
        .data(visibleLinks)
        .join('text')
        .attr('class', 'ontology-link-label')
        .attr('text-anchor', 'middle')
        .text(d => {
          const relation = String(d.relation || '').toUpperCase();
          if (relation === 'REPORT_HAS_MDRM') return 'comprises of';
          if (relation === 'MDRM_HAS_RULE') return 'governed by';
          if (relation === 'RULE_REQUIRES_MDRM') return 'depends on';
          return 'can be of';
        });

      setOntologyGraphNodeSelection(svg.append('g')
        .attr('class', 'ontology-nodes')
        .selectAll('g')
        .data(visibleNodes)
        .join('g')
        .attr('class', d => ontologyNodeClass(d))
        .on('click', (_, d) => {
          setOntologySelectedNodeId(d.id);
          const category = normalizeOntologyCategory(d.category);
          if (category === 'REPORT') {
            const reportLabel = String(d.label || '').trim();
            setOntologyFocusedReport(reportLabel);
            setOntologyFocusedMdrm('');
            setActiveOntologyInspectorTab('reports');
            syncOntologyRuleScope({ reports: [reportLabel], mdrms: [] }).catch(() => {});
          } else if (category === 'MDRM') {
            const mdrmCode = String(d.label || '').trim().toUpperCase();
            const connectedReports = Array.from(getOntologyNeighborMap().get(d.id) || [])
              .map(id => getOntologyNodeById().get(id))
              .filter(Boolean)
              .filter(node => normalizeOntologyCategory(node.category) === 'REPORT')
              .map(node => String(node.label || '').trim())
              .filter(Boolean);
            if (!getEffectiveSelectedReports().length && connectedReports.length === 1) {
              setOntologyFocusedReport(connectedReports[0]);
            }
            setOntologyFocusedMdrm(mdrmCode);
            setActiveOntologyInspectorTab('mdrms');
            syncOntologyRuleScope({
              reports: getEffectiveSelectedReports(),
              mdrms: [mdrmCode]
            }).catch(() => {});
          } else if (category === 'RULE') {
            setActiveOntologyInspectorTab('rules');
          } else {
            setActiveOntologyInspectorTab('summary');
          }
          renderOntologyDetail(d);
          renderOntologyScopeChips();
          refreshOntologyHighlight();
          syncOntologyManageActions();
        }));

      getOntologyGraphNodeSelection().append('circle').attr('r', d => ontologyNodeRadius(d));
      getOntologyGraphNodeSelection().append('text')
        .attr('dy', d => normalizeOntologyCategory(d.category) === 'MDRM' ? 3 : 4)
        .attr('text-anchor', 'middle')
        .text(d => shortenOntologyLabel(d.label, d.category));
      getOntologyGraphNodeSelection().append('title')
        .text(d => `${d.label || '-'} | ${d.category || '-'}` + (d.status ? ` | ${d.status}` : ''));

      if (deps.isAdminUser()) {
        const graphManageIcon = getOntologyGraphNodeSelection()
          .filter(d => normalizeOntologyCategory(d.category) === 'MDRM')
          .append('g')
          .attr('class', 'ontology-node-info ontology-node-edit')
          .attr('transform', 'translate(-20,-20)')
          .on('click', (event, d) => {
            event.stopPropagation();
            const connectedReports = Array.from(getOntologyNeighborMap().get(d.id) || [])
              .map(id => getOntologyNodeById().get(id))
              .filter(Boolean)
              .filter(node => normalizeOntologyCategory(node.category) === 'REPORT');
            const report = connectedReports.length === 1
              ? String(connectedReports[0].label || '').trim()
              : selectedOntologyReportForManage();
            const mdrmCode = String(d.label || '').trim().toUpperCase();
            openOntologyManageModal({
              operation: 'EDIT',
              report,
              mdrm: mdrmCode
            });
          });

        graphManageIcon.append('circle').attr('r', 7);
        graphManageIcon.append('text')
          .attr('text-anchor', 'middle')
          .attr('dy', 3)
          .text('✎');
      }

      const drag = d3.drag()
        .on('start', (event, d) => {
          if (!event.active) getOntologyGraphSimulation().alphaTarget(0.2).restart();
          d.fx = d.x;
          d.fy = d.y;
        })
        .on('drag', (event, d) => {
          d.fx = event.x;
          d.fy = event.y;
        })
        .on('end', (event, d) => {
          if (!event.active) getOntologyGraphSimulation().alphaTarget(0);
          d.fx = null;
          d.fy = null;
        });
      getOntologyGraphNodeSelection().call(drag);

      setOntologyGraphSimulation(d3.forceSimulation(visibleNodes)
        .force('link', d3.forceLink(visibleLinks).id(d => d.id).distance(() => 130))
        .force('charge', d3.forceManyBody().strength(-280))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('collision', d3.forceCollide().radius(d => ontologyNodeRadius(d) + 10))
        .on('tick', () => {
          getOntologyGraphLinkSelection()
            .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
          linkLabelSelection
            .attr('x', d => (d.source.x + d.target.x) / 2)
            .attr('y', d => ((d.source.y + d.target.y) / 2) - 8);
          getOntologyGraphNodeSelection().attr('transform', d => `translate(${d.x}, ${d.y})`);
        }));
      renderOntologyDetail(null);
      refreshOntologyHighlight();
    }

    function closeOntologySelector() {
      if (!el.ontologySelectorOverlay) return;
      el.ontologySelectorOverlay.classList.remove('is-open');
      el.ontologySelectorOverlay.hidden = true;
      setOntologySelectorMode(null);
      setOntologySelectorOptions([]);
      setOntologySelectorWorkingSet(new Set());
    }

    function renderOntologySelectorOptions() {
      if (!el.ontologySelectorList) return;
      const q = String(el.ontologySelectorSearch?.value || '').trim().toLowerCase();
      const filtered = (getOntologySelectorOptions() || []).filter(item => !q || String(item).toLowerCase().includes(q));
      if (!filtered.length) {
        el.ontologySelectorList.innerHTML = '<div class="command-palette-empty">No matching options</div>';
        return;
      }
      el.ontologySelectorList.innerHTML = filtered.map(item => `
        <label class="ontology-option-row">
          <input type="checkbox" data-value="${deps.escapeHtml(item)}" ${getOntologySelectorWorkingSet().has(item) ? 'checked' : ''}>
          <span>${deps.escapeHtml(item)}</span>
        </label>
      `).join('');
      el.ontologySelectorList.querySelectorAll('input[type="checkbox"]').forEach(input => {
        input.addEventListener('change', () => {
          const value = String(input.getAttribute('data-value') || '');
          if (!value) return;
          if (input.checked) getOntologySelectorWorkingSet().add(value);
          else getOntologySelectorWorkingSet().delete(value);
          if (el.ontologySelectorMeta) {
            el.ontologySelectorMeta.textContent = `${getOntologySelectorWorkingSet().size} selected`;
          }
        });
      });
    }

    async function openOntologySelector(mode) {
      if (!el.ontologySelectorOverlay) return;
      const normalizedMode = mode === 'mdrm' ? 'mdrm' : 'report';
      setOntologyStatus(`Loading ${normalizedMode === 'report' ? 'report' : 'MDRM'} options...`, 'neutral');
      try {
        const optionsUrl = normalizedMode === 'report'
          ? buildOntologyUrl('/api/mdrm/ontology-options', { includeReportFilters: false, includeMdrmFilters: false })
          : buildOntologyUrl('/api/mdrm/ontology-options', { includeMdrmFilters: false });
        const response = await fetch(optionsUrl);
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.message || `HTTP ${response.status}`);
        }
        setOntologySelectorMode(normalizedMode);
        setOntologySelectorOptions(normalizedMode === 'report'
          ? (Array.isArray(payload?.reports) ? payload.reports : [])
          : (Array.isArray(payload?.mdrms) ? payload.mdrms : []));
        setOntologySelectorWorkingSet(new Set(
          normalizedMode === 'report' ? (getOntologySelectedReportsState() || []) : (getOntologySelectedMdrmsState() || [])
        ));
        el.ontologySelectorOverlay.hidden = false;
        window.requestAnimationFrame(() => el.ontologySelectorOverlay.classList.add('is-open'));
        if (el.ontologySelectorSearch) {
          el.ontologySelectorSearch.value = '';
          el.ontologySelectorSearch.focus();
        }
        if (el.ontologySelectorMeta) {
          el.ontologySelectorMeta.textContent = `${getOntologySelectorWorkingSet().size} selected`;
        }
        renderOntologySelectorOptions();
        setOntologyStatus(
          normalizedMode === 'report'
            ? 'Select reports and apply to build graph.'
            : 'Select MDRMs and apply to refine graph.',
          'ok'
        );
      } catch (error) {
        setOntologyStatus(`Unable to load selector options: ${error.message || error}`, 'err');
      }
    }

    async function applyOntologySelectorSelection() {
      if (!getOntologySelectorMode()) return;
      const selected = Array.from(getOntologySelectorWorkingSet()).sort((a, b) => a.localeCompare(b));
      if (getOntologySelectorMode() === 'report') {
        setOntologySelectedReportsState(selected);
        setOntologySelectedMdrmsState([]);
        setOntologyFocusedReport(selected.length === 1 ? selected[0] : '');
        setOntologyFocusedMdrm('');
      } else {
        setOntologySelectedMdrmsState(selected);
        setOntologyFocusedMdrm(selected.length === 1 ? String(selected[0] || '').trim().toUpperCase() : '');
      }
      closeOntologySelector();
      await loadOntologyGraph(true);
    }

    async function loadOntologyGraph(force = false) {
      if (!el.ontologyGraphHost) return;
      const signature = [
        getOntologyMode(),
        Number(deps.getRunContextId() || 0),
        (getOntologySelectedReportsState() || []).join('|'),
        (getOntologySelectedMdrmsState() || []).join('|')
      ].join('::');
      if (!force && getOntologyLoadedContextRunId() === signature && getOntologyGraphPayload()) return;

      setOntologyStatus('Loading ontology summary...', 'neutral');
      try {
        const selectedReports = (getOntologySelectedReportsState() || []).map(v => String(v).trim()).filter(Boolean);
        if (selectedReports.length === 1) {
          setOntologyFocusedReport(selectedReports[0]);
        } else if (!selectedReports.length) {
          setOntologyFocusedReport('');
        }
        const selectedMdrms = (getOntologySelectedMdrmsState() || []).map(v => String(v).trim()).filter(Boolean);
        if (selectedMdrms.length === 1) {
          setOntologyFocusedMdrm(selectedMdrms[0].toUpperCase());
        } else if (!selectedMdrms.length) {
          setOntologyFocusedMdrm('');
        }
        const baseResponse = await fetch(buildOntologyBaseSummaryUrl());
        const basePayload = await baseResponse.json();
        if (!baseResponse.ok) {
          throw new Error(basePayload?.message || `HTTP ${baseResponse.status}`);
        }
        setOntologyBaseSummaryPayload(basePayload || null);
        setOntologyLoadedContextRunId(signature);

        let scopedPayload = null;
        if ((getOntologySelectedReportsState() || []).length || (getOntologySelectedMdrmsState() || []).length) {
          const scopeResponse = await fetch(buildOntologyUrl('/api/mdrm/ontology-graph', { summaryOnly: true }));
          scopedPayload = await scopeResponse.json();
          if (!scopeResponse.ok) {
            throw new Error(scopedPayload?.message || `HTTP ${scopeResponse.status}`);
          }
        }

        const displayPayload = scopedPayload || basePayload;
        await loadOntologyRuleSummaryPayload(selectedReports, selectedMdrms);
        setOntologyGraphPayload(displayPayload || null);
        renderOntologySummaryGraph(displayPayload);
        renderOntologyReferenceTables();
        syncOntologyManageActions();

        if (!scopedPayload) {
          setOntologyStatus('Summary loaded. Select reports or MDRMs to continue.', 'ok');
        } else {
          const ruleSummaryPayload = getOntologyRuleSummaryPayload();
          setOntologyStatus(
            `Selection updated | reports=${deps.formatCount(scopedPayload?.reportCount || 0)} | active MDRMs=${deps.formatCount(scopedPayload?.activeMdrmCount || scopedPayload?.mdrmCount || 0)}${ruleSummaryPayload ? ` | rules=${deps.formatCount(ruleSummaryPayload?.totalRules || 0)}` : ''}`,
            'ok'
          );
        }
      } catch (error) {
        setOntologyGraphPayload(null);
        setOntologyBaseSummaryPayload(null);
        setOntologyRuleSummaryPayload(null);
        setOntologyLoadedContextRunId(null);
        clearOntologyGraph('Unable to load ontology graph.');
        renderOntologyReferenceTables();
        syncOntologyManageActions();
        setOntologyStatus(`Unable to load ontology graph: ${error.message || error}`, 'err');
      }
    }

    initOntologyPaneResize();

    return {
      resetOntologyScopeState,
      setOntologyStatus,
      normalizeOntologyCategory,
      ontologyNodeRadius,
      shortenOntologyLabel,
      ontologyNodeClass,
      buildOntologyUrl,
      buildOntologyBaseSummaryUrl,
      setActiveOntologyInspectorTab,
      renderOntologyReferenceTables,
      openOntologyInsightShell,
      closeOntologyInsight,
      openOntologyInsight,
      loadOntologyReportInsight,
      loadOntologyMdrmInsight,
      selectedOntologyReportForManage,
      selectedOntologyMdrmForManage,
      isLatestRunSelectedForManage,
      syncOntologyManageActions,
      syncOntologyManageModalFields,
      loadOntologyManageMdrmOptions,
      openOntologyManageModal,
      submitOntologyManageModal,
      clearOntologyGraph,
      removeOntologyInfoNote,
      toggleOntologyInfoNote,
      splitOntologyNodeLabel,
      wrapOntologyText,
      appendOntologyNodeLabels,
      renderOntologySummaryGraph,
      renderOntologyDetail,
      refreshOntologyHighlight,
      resetOntologyView,
      clearOntologyView,
      renderOntologyGraph,
      closeOntologySelector,
      renderOntologySelectorOptions,
      openOntologySelector,
      applyOntologySelectorSelection,
      loadOntologyGraph
    };
  }

  window.ProjectAiUiModules = Object.assign(window.ProjectAiUiModules || {}, {
    createOntologyManager
  });
})();
