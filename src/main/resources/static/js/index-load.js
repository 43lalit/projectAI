(function () {
  function createLoadManager(deps) {
    const el = deps.elements;

    function setLoadStatus(text, css) {
      el.loadStatus.textContent = text;
      el.loadStatus.className = `status ${css}`;
    }

    function setLoadActionsDisabled(disabled) {
      const admin = deps.isAdminUser();
      el.runLoadBtn.disabled = disabled || !admin || !deps.hasSelectedUploadFile();
      el.uploadFileBtn.disabled = disabled || !admin;
      el.uploadFileInput.disabled = disabled || !admin;
    }

    function renderFileMetrics(runs) {
      if (!runs || !runs.length) {
        el.fileMetricsContainer.innerHTML = '<div class="text-muted small p-2">No runs available for the selected as-of context.</div>';
        return;
      }
      const rowsHtml = runs.map(run => `
        <tr>
          <td>${deps.escapeHtml(run.runId)}</td>
          <td>${deps.escapeHtml(deps.formatRunDate(run.runDatetime))}</td>
          <td>${deps.escapeHtml(run.fileName)}</td>
          <td>${deps.escapeHtml(run.numFileRecords)}</td>
          <td>${deps.escapeHtml(run.numRecordsIngested)}</td>
          <td>${deps.escapeHtml(run.numRecordsError)}</td>
          <td>${deps.escapeHtml(run.reportsCount)}</td>
          <td>${deps.escapeHtml(run.totalUniqueMdrms)}</td>
          <td>${deps.escapeHtml(run.activeMdrms)}</td>
          <td>${deps.escapeHtml(run.inactiveMdrms)}</td>
          <td>${deps.escapeHtml(run.updatedMdrms)}</td>
          <td><a class="file-inc-link" href="#" data-run-id="${deps.escapeHtml(run.runId)}">View</a></td>
        </tr>
      `).join('');

      el.fileMetricsContainer.innerHTML = `
        <table class="table table-sm table-striped table-hover mb-0">
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Run Date</th>
              <th>Source File</th>
              <th>File Rows</th>
              <th>Ingested</th>
              <th>Error</th>
              <th>Reports</th>
              <th>Total MDRMs</th>
              <th>Active</th>
              <th>Inactive</th>
              <th>Updated</th>
              <th>Incremental</th>
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      `;
    }

    function renderFileIncremental(runId, rows) {
      el.fileIncrementalTitle.textContent = `Run ${runId} incremental changes by report`;
      el.fileIncrementalTitle.className = 'status ok mt-2';
      if (!rows || !rows.length) {
        el.fileIncrementalContainer.innerHTML = '<div class="text-muted small p-2">No incremental changes for this run.</div>';
        el.fileIncrementalDetailPanel.innerHTML = '<div class="text-muted small p-2">Select a report row to view MDRM details.</div>';
        return;
      }
      const body = rows.map(row => `
        <tr>
          <td>${deps.escapeHtml(row.reportingForm)}</td>
          <td>${deps.escapeHtml(row.addedMdrms)}</td>
          <td>${deps.escapeHtml(row.modifiedMdrms)}</td>
          <td>${deps.escapeHtml(row.deletedMdrms)}</td>
          <td><a class="inc-report-link" href="#" data-run-id="${deps.escapeHtml(runId)}" data-reporting-form="${deps.escapeHtml(row.reportingForm)}">Details</a></td>
        </tr>
      `).join('');
      el.fileIncrementalContainer.innerHTML = `
        <table class="table table-sm table-striped table-hover mb-0">
          <thead>
            <tr>
              <th>Reporting Form</th>
              <th>Added</th>
              <th>Modified</th>
              <th>Deleted</th>
              <th>View</th>
            </tr>
          </thead>
          <tbody>${body}</tbody>
        </table>
      `;
      el.fileIncrementalDetailPanel.innerHTML = '<div class="text-muted small p-2">Select a report row to view MDRM details.</div>';
    }

    function renderIncrementalDetailPanel(runId, reportingForm, addedCodes, modifiedCodes, deletedCodes) {
      const sectionIdPrefix = `inc-${runId}-${reportingForm.replaceAll(/[^a-zA-Z0-9]/g, '_')}`;
      const buildRows = codes => {
        if (!codes.length) {
          return '<div class="text-muted small">No MDRMs</div>';
        }
        const selectedRun = deps.getRunContextId();
        return `<div class="small" style="max-height: 220px; overflow: auto;"><ul class="mb-0">${codes.map(code => {
          const params = new URLSearchParams();
          params.set('mdrm', String(code));
          if (selectedRun) {
            params.set('runId', String(selectedRun));
          }
          return `<li><a href="/mdrm.html?${params.toString()}">${deps.escapeHtml(code)}</a></li>`;
        }).join('')}</ul></div>`;
      };

      el.fileIncrementalDetailPanel.innerHTML = `
        <div class="p-2">
          <div class="fw-semibold mb-2">Run ${deps.escapeHtml(runId)} | ${deps.escapeHtml(reportingForm)}</div>
          <div class="accordion" id="${sectionIdPrefix}-accordion">
            <div class="accordion-item">
              <h2 class="accordion-header">
                <button class="accordion-button" type="button" data-bs-toggle="collapse" data-bs-target="#${sectionIdPrefix}-added" aria-expanded="true">
                  Added MDRMs (${addedCodes.length})
                </button>
              </h2>
              <div id="${sectionIdPrefix}-added" class="accordion-collapse collapse show" data-bs-parent="#${sectionIdPrefix}-accordion">
                <div class="accordion-body">${buildRows(addedCodes)}</div>
              </div>
            </div>
            <div class="accordion-item">
              <h2 class="accordion-header">
                <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#${sectionIdPrefix}-modified" aria-expanded="false">
                  Modified MDRMs (${modifiedCodes.length})
                </button>
              </h2>
              <div id="${sectionIdPrefix}-modified" class="accordion-collapse collapse" data-bs-parent="#${sectionIdPrefix}-accordion">
                <div class="accordion-body">${buildRows(modifiedCodes)}</div>
              </div>
            </div>
            <div class="accordion-item">
              <h2 class="accordion-header">
                <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#${sectionIdPrefix}-deleted" aria-expanded="false">
                  Deleted MDRMs (${deletedCodes.length})
                </button>
              </h2>
              <div id="${sectionIdPrefix}-deleted" class="accordion-collapse collapse" data-bs-parent="#${sectionIdPrefix}-accordion">
                <div class="accordion-body">${buildRows(deletedCodes)}</div>
              </div>
            </div>
          </div>
        </div>
      `;
    }

    async function runLoad() {
      if (!deps.isAdminUser()) {
        setLoadStatus('Admin access required to load MDRM files.', 'err');
        return;
      }
      setLoadActionsDisabled(true);
      setLoadStatus('Loading... calling /api/mdrm/load', 'neutral');
      try {
        const response = await deps.apiFetch('/api/mdrm/load', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        const bodyText = await response.text();
        let payload;
        try {
          payload = bodyText ? JSON.parse(bodyText) : null;
        } catch {
          payload = bodyText;
        }
        if (!response.ok) {
          setLoadStatus(
            `Load failed (HTTP ${response.status})\n${typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2)}`,
            'err'
          );
          return;
        }
        setLoadStatus(`Load successful\n${JSON.stringify(payload, null, 2)}`, 'ok');
        deps.setFormsLoaded(false);
        await refreshFileMetrics();
      } catch (error) {
        setLoadStatus(`Request error\n${error.message}`, 'err');
      } finally {
        setLoadActionsDisabled(false);
      }
    }

    async function uploadFile() {
      if (!deps.isAdminUser()) {
        setLoadStatus('Admin access required to upload MDRM files.', 'err');
        return;
      }
      const file = el.uploadFileInput.files && el.uploadFileInput.files[0];
      if (!file) {
        setLoadStatus('Select an MDRM_ddyy.csv file first.', 'err');
        return;
      }
      const form = new FormData();
      form.append('file', file);

      setLoadActionsDisabled(true);
      setLoadStatus(`Uploading ${file.name}...`, 'neutral');
      try {
        const response = await deps.apiFetch('/api/mdrm/upload', {
          method: 'POST',
          body: form
        });
        const bodyText = await response.text();
        let payload;
        try {
          payload = bodyText ? JSON.parse(bodyText) : null;
        } catch {
          payload = bodyText;
        }
        if (!response.ok) {
          setLoadStatus(
            `Upload failed (HTTP ${response.status})\n${typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2)}`,
            'err'
          );
          return;
        }
        setLoadStatus(`Upload successful\n${JSON.stringify(payload, null, 2)}`, 'ok');
        deps.setFormsLoaded(false);
        el.uploadFileInput.value = '';
        deps.updateLoadButtonStates();
        await refreshFileMetrics();
      } catch (error) {
        setLoadStatus(`Upload error\n${error.message}`, 'err');
      } finally {
        setLoadActionsDisabled(false);
      }
    }

    async function refreshFileMetrics() {
      try {
        const response = await fetch('/api/mdrm/file-runs');
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${JSON.stringify(payload)}`);
        }
        const allRuns = Array.isArray(payload) ? payload : [];
        const visibleRuns = deps.filterRunsByContext(allRuns);
        renderFileMetrics(visibleRuns);
        deps.loadRunContextOptions().catch(() => {});
        if (el.kpiMdrmCount) {
          el.kpiMdrmCount.textContent = deps.formatCount(0);
          try {
            const kpiResponse = await fetch(deps.appendRunContext('/api/mdrm/ontology-graph?summaryOnly=true'));
            const kpiPayload = await kpiResponse.json();
            if (kpiResponse.ok) {
              el.kpiMdrmCount.textContent = deps.formatCount(kpiPayload?.mdrmCount || 0);
            } else if (visibleRuns.length) {
              const latestRun = visibleRuns.reduce((latest, run) => {
                if (!latest) return run;
                return Number(run.runDatetime || 0) > Number(latest.runDatetime || 0) ? run : latest;
              }, null);
              el.kpiMdrmCount.textContent = deps.formatCount((latestRun && latestRun.totalUniqueMdrms) ?? 0);
            }
          } catch {
            if (visibleRuns.length) {
              const latestRun = visibleRuns.reduce((latest, run) => {
                if (!latest) return run;
                return Number(run.runDatetime || 0) > Number(latest.runDatetime || 0) ? run : latest;
              }, null);
              el.kpiMdrmCount.textContent = deps.formatCount((latestRun && latestRun.totalUniqueMdrms) ?? 0);
            }
          }
        }
      } catch (_) {
        el.fileMetricsContainer.innerHTML = '<div class="text-danger small p-2">Failed to load file metrics.</div>';
        if (el.kpiMdrmCount) {
          el.kpiMdrmCount.textContent = deps.formatCount(0);
        }
      }
    }

    async function loadFileIncremental(runId) {
      el.fileIncrementalTitle.textContent = `Loading incremental changes for run ${runId}...`;
      el.fileIncrementalTitle.className = 'status neutral mt-2';
      el.fileIncrementalContainer.innerHTML = '';
      el.fileIncrementalDetailPanel.innerHTML = '<div class="text-muted small p-2">Select a report row to view MDRM details.</div>';
      try {
        const response = await fetch(`/api/mdrm/incremental-summary?runId=${encodeURIComponent(runId)}`);
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${JSON.stringify(payload)}`);
        }
        renderFileIncremental(runId, Array.isArray(payload) ? payload : []);
      } catch (_) {
        el.fileIncrementalTitle.textContent = `Failed to load incremental changes for run ${runId}`;
        el.fileIncrementalTitle.className = 'status err mt-2';
      }
    }

    async function loadIncrementalReportDetails(runId, reportingForm) {
      el.fileIncrementalDetailPanel.innerHTML = `<div class="text-muted small p-2">Loading details for ${deps.escapeHtml(reportingForm)}...</div>`;
      const queryForm = encodeURIComponent(reportingForm);
      const queryRun = encodeURIComponent(runId);
      try {
        const [addedRes, modifiedRes, deletedRes] = await Promise.all([
          fetch(`/api/mdrm/run-incremental-mdrms?reportingForm=${queryForm}&runId=${queryRun}&changeType=ADDED`),
          fetch(`/api/mdrm/run-incremental-mdrms?reportingForm=${queryForm}&runId=${queryRun}&changeType=MODIFIED`),
          fetch(`/api/mdrm/run-incremental-mdrms?reportingForm=${queryForm}&runId=${queryRun}&changeType=DELETED`)
        ]);
        const [addedPayload, modifiedPayload, deletedPayload] = await Promise.all([
          addedRes.json(),
          modifiedRes.json(),
          deletedRes.json()
        ]);
        if (!addedRes.ok || !modifiedRes.ok || !deletedRes.ok) {
          throw new Error('Failed to load one or more incremental MDRM lists');
        }
        renderIncrementalDetailPanel(
          runId,
          reportingForm,
          Array.isArray(addedPayload.mdrmCodes) ? addedPayload.mdrmCodes : [],
          Array.isArray(modifiedPayload.mdrmCodes) ? modifiedPayload.mdrmCodes : [],
          Array.isArray(deletedPayload.mdrmCodes) ? deletedPayload.mdrmCodes : []
        );
      } catch (_) {
        el.fileIncrementalDetailPanel.innerHTML = `<div class="text-danger small p-2">Failed to load details for ${deps.escapeHtml(reportingForm)}.</div>`;
      }
    }

    return {
      setLoadStatus,
      setLoadActionsDisabled,
      renderFileMetrics,
      renderFileIncremental,
      renderIncrementalDetailPanel,
      runLoad,
      uploadFile,
      refreshFileMetrics,
      loadFileIncremental,
      loadIncrementalReportDetails
    };
  }

  window.ProjectAiUiModules = Object.assign(window.ProjectAiUiModules || {}, {
    createLoadManager
  });
})();
