(function () {
  function createCommandPaletteManager(deps) {
    const COMMAND_STOP_WORDS = new Set([
      'go', 'to', 'open', 'show', 'me', 'please', 'the', 'a', 'an', 'for', 'in', 'on', 'with'
    ]);

    let actions = [];
    let selectionIndex = 0;
    let closeTimer = null;

    function isOpen() {
      return !!(deps.overlay && !deps.overlay.hidden);
    }

    function tokenizeQuery(value) {
      return String(value || '')
        .toLowerCase()
        .split(/[^a-z0-9]+/)
        .map(token => token.trim())
        .filter(token => token && !COMMAND_STOP_WORDS.has(token));
    }

    function normalizeKey(value) {
      return String(value || '')
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '');
    }

    function parseMdrmCode(query) {
      const raw = String(query || '').trim();
      if (!raw) {
        return '';
      }
      const explicit = raw.match(/\bmdrm[:\s-]*([a-z0-9]{4,12})\b/i);
      if (explicit && explicit[1]) {
        return explicit[1].toUpperCase();
      }
      const compact = raw.match(/^[a-z][a-z0-9]{5,11}$/i);
      if (compact && /[a-z]/i.test(raw) && /\d/.test(raw)) {
        return raw.toUpperCase();
      }
      return '';
    }

    function buildContextualActions(query) {
      const raw = String(query || '').trim();
      if (!raw) {
        return [];
      }

      const actionItems = [];
      const normalizedQuery = normalizeKey(raw);

      actionItems.push({
        id: 'search-discovery-' + raw.toLowerCase(),
        label: 'Search Discovery: ' + raw,
        hint: 'Run semantic search in discovery',
        keywords: 'search discovery ' + raw.toLowerCase(),
        run: async () => {
          deps.switchView('view-discovery');
          const discoveryInput = deps.getDiscoveryInput();
          if (discoveryInput) {
            discoveryInput.value = raw;
          }
          await deps.runDiscoverySearch();
        }
      });

      const reportCandidates = (deps.getAllForms() || [])
        .map(form => {
          const normalizedForm = normalizeKey(form);
          if (!normalizedForm) {
            return null;
          }

          let score = 0;
          if (normalizedQuery && normalizedForm === normalizedQuery) {
            score += 100;
          }
          if (normalizedQuery && normalizedForm.startsWith(normalizedQuery)) {
            score += 70;
          }
          if (normalizedQuery && normalizedForm.includes(normalizedQuery)) {
            score += 50;
          }

          tokenizeQuery(raw)
            .map(token => token.toUpperCase())
            .filter(token => token.length >= 2)
            .forEach(token => {
              if (normalizedForm.includes(token)) {
                score += token.length >= 4 ? 15 : 8;
              }
            });

          return score > 0 ? { form, score } : null;
        })
        .filter(Boolean)
        .sort((a, b) => b.score - a.score || a.form.localeCompare(b.form))
        .slice(0, 6);

      reportCandidates.forEach(candidate => {
        actionItems.push({
          id: 'open-report-' + candidate.form,
          label: 'Open Report: ' + candidate.form,
          hint: 'Go to report details and timeline',
          keywords: 'open report ' + candidate.form.toLowerCase(),
          run: () => deps.openReportFromPalette(candidate.form)
        });
      });

      const mdrmCode = parseMdrmCode(raw);
      const hasExplicitMdrmIntent = /\bmdrm\b/i.test(raw);
      const mdrmLooksLikeTopReport = !!reportCandidates.find(
        candidate => normalizeKey(candidate.form) === normalizeKey(mdrmCode)
      );
      if (mdrmCode && (hasExplicitMdrmIntent || !mdrmLooksLikeTopReport)) {
        actionItems.push({
          id: 'open-mdrm-' + mdrmCode,
          label: 'Open MDRM: ' + mdrmCode,
          hint: 'Open MDRM profile page',
          keywords: 'open mdrm ' + mdrmCode.toLowerCase(),
          run: () => {
            window.location.href = deps.buildMdrmProfileUrl(mdrmCode);
          }
        });
      }

      return actionItems;
    }

    function buildStaticActions() {
      const actionItems = [
        {
          id: 'view-discovery',
          label: 'Go to Discovery',
          hint: 'Open search and discovery workspace',
          keywords: 'discovery search',
          run: () => deps.switchView('view-discovery')
        },
        {
          id: 'view-bookmarks',
          label: 'Go to Bookmarks',
          hint: 'Open bookmark groups and saved MDRMs',
          keywords: 'bookmarks favorites',
          run: () => deps.switchView('view-bookmarks')
        },
        {
          id: 'view-reporting',
          label: 'Go to Reports',
          hint: 'Open reporting viewer',
          keywords: 'reports reporting run history',
          run: () => deps.switchView('view-reporting')
        },
        {
          id: 'view-ontology',
          label: 'Go to Ontology',
          hint: 'Open Report -> MDRM -> Type graph',
          keywords: 'ontology graph relationship mindmap',
          run: () => deps.switchView('view-ontology')
        },
        {
          id: 'focus-discovery',
          label: 'Focus Discovery Search',
          hint: 'Jump to discovery input',
          keywords: 'focus find lookup',
          run: () => {
            deps.switchView('view-discovery');
            const discoveryInput = deps.getDiscoveryInput();
            discoveryInput?.focus();
            discoveryInput?.select();
          }
        },
        {
          id: 'focus-reports',
          label: 'Focus Report Search',
          hint: 'Jump to report filter input',
          keywords: 'focus reports find',
          run: () => {
            deps.switchView('view-reporting');
            const reportInput = deps.getReportSearchInput();
            reportInput?.focus();
            reportInput?.select();
          }
        },
        {
          id: 'toggle-theme',
          label: 'Toggle Theme',
          hint: 'Switch light or dark mode',
          keywords: 'theme dark light',
          run: () => {
            const currentTheme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
            deps.setTheme(currentTheme === 'dark' ? 'light' : 'dark');
          }
        },
        {
          id: 'quick-tour',
          label: 'Start Quick Tour',
          hint: 'Launch onboarding guide',
          keywords: 'guide onboarding tour help',
          run: () => deps.openGuide(0)
        }
      ];

      if (deps.isAdminUser()) {
        actionItems.push({
          id: 'view-load',
          label: 'Go to Load Data',
          hint: 'Open MDRM upload and run screen',
          keywords: 'load upload admin',
          run: () => deps.switchView('view-load')
        });
      }

      if (!deps.getCurrentUser()) {
        actionItems.push({
          id: 'open-login',
          label: 'Open Login',
          hint: 'Open profile menu in login mode',
          keywords: 'login sign in auth',
          run: () => {
            const profileMenu = deps.getProfileMenu();
            const profileMenuButton = deps.getProfileMenuButton();
            profileMenu?.classList.add('open');
            profileMenuButton?.setAttribute('aria-expanded', 'true');
            deps.setAuthMode('login');
            deps.getAuthEmailInput()?.focus();
          }
        });
      }

      const runContextSelect = deps.getRunContextSelect();
      if (runContextSelect) {
        Array.from(runContextSelect.options || [])
          .map(option => ({
            value: Number(option.value),
            label: String(option.textContent || '').trim()
          }))
          .filter(option => Number.isFinite(option.value) && option.value > 0)
          .slice(0, 8)
          .forEach(option => {
            actionItems.push({
              id: 'run-' + option.value,
              label: 'Set Context: ' + option.label,
              hint: 'Switch as-of run context',
              keywords: 'context run asof ' + option.label.toLowerCase(),
              run: async () => {
                const select = deps.getRunContextSelect();
                if (!select) {
                  return;
                }
                select.value = String(option.value);
                await deps.applyRunContextChange();
              }
            });
          });
      }

      (deps.getSavedSearches() || []).slice(0, 5).forEach((entry, idx, entries) => {
        const resolvedIndex = entries.indexOf(entry);
        if (resolvedIndex < 0) {
          return;
        }
        actionItems.push({
          id: 'saved-search-' + resolvedIndex,
          label: 'Saved Search: ' + String(entry.name || ('Search ' + (idx + 1))),
          hint: 'Apply saved discovery search',
          keywords: 'saved search ' + String(entry.name || '').toLowerCase() + ' ' + String(entry.state?.q || '').toLowerCase(),
          run: () => deps.applySavedSearchByIndex(resolvedIndex)
        });
      });

      (deps.getRecentSearches() || []).slice(0, 5).forEach((entry, idx, entries) => {
        const resolvedIndex = entries.indexOf(entry);
        const query = String(entry && entry.query ? entry.query : '').trim();
        if (resolvedIndex < 0 || !query) {
          return;
        }
        actionItems.push({
          id: 'recent-search-' + resolvedIndex,
          label: 'Recent Search: ' + query,
          hint: 'Re-run this search',
          keywords: 'recent search ' + query.toLowerCase(),
          run: () => deps.applyRecentSearchByIndex(resolvedIndex)
        });
      });

      return actionItems;
    }

    function renderResults() {
      if (!deps.list || !deps.meta) {
        return;
      }
      if (!actions.length) {
        deps.meta.textContent = 'No matching commands';
        deps.list.innerHTML = '<div class="command-palette-empty">No results</div>';
        return;
      }

      deps.meta.textContent = String(actions.length) + ' command' + (actions.length === 1 ? '' : 's');
      selectionIndex = Math.max(0, Math.min(selectionIndex, actions.length - 1));
      deps.list.innerHTML = actions.map((action, index) => (
        '<button class="command-palette-item ' + (index === selectionIndex ? 'active' : '') + '" type="button" data-index="' + index + '">' +
          '<span class="command-palette-item-label">' + deps.escapeHtml(action.label) + '</span>' +
          '<span class="command-palette-item-hint">' + deps.escapeHtml(action.hint || '') + '</span>' +
        '</button>'
      )).join('');
    }

    function refresh(query) {
      const rawQuery = String(query || '').trim();
      const loweredQuery = rawQuery.toLowerCase();
      const allActions = [];
      const seenIds = new Set();

      [...buildContextualActions(rawQuery), ...buildStaticActions()].forEach(action => {
        const id = String(action && action.id ? action.id : '');
        if (!id || seenIds.has(id)) {
          return;
        }
        seenIds.add(id);
        allActions.push(action);
      });

      if (!loweredQuery) {
        actions = allActions;
        selectionIndex = 0;
        renderResults();
        return;
      }

      const tokens = tokenizeQuery(loweredQuery);
      if (!tokens.length) {
        actions = allActions;
        selectionIndex = 0;
        renderResults();
        return;
      }

      actions = allActions.filter(action => {
        const haystack = (action.label + ' ' + (action.hint || '') + ' ' + (action.keywords || '')).toLowerCase();
        return tokens.every(token => haystack.includes(token));
      });
      selectionIndex = 0;
      renderResults();
    }

    function open(initialQuery) {
      if (!deps.overlay || !deps.input) {
        return;
      }
      if (closeTimer) {
        window.clearTimeout(closeTimer);
        closeTimer = null;
      }
      deps.closeProfileMenu();
      deps.overlay.hidden = false;
      window.requestAnimationFrame(() => deps.overlay.classList.add('is-open'));
      deps.input.value = initialQuery || '';
      refresh(initialQuery || '');
      deps.input.focus();
      deps.input.select();
    }

    function close() {
      if (!deps.overlay) {
        return;
      }
      deps.overlay.classList.remove('is-open');
      if (closeTimer) {
        window.clearTimeout(closeTimer);
      }
      closeTimer = window.setTimeout(() => {
        deps.overlay.hidden = true;
        closeTimer = null;
      }, 120);
    }

    function moveSelection(delta) {
      if (!actions.length) {
        return;
      }
      const nextIndex = selectionIndex + delta;
      if (nextIndex < 0) {
        selectionIndex = actions.length - 1;
      } else if (nextIndex >= actions.length) {
        selectionIndex = 0;
      } else {
        selectionIndex = nextIndex;
      }
      renderResults();
    }

    async function executeSelection(index) {
      const resolvedIndex = Number.isFinite(index) ? index : selectionIndex;
      if (!actions.length) {
        return;
      }

      const paletteQuery = String(deps.input?.value || '').trim();
      if (paletteQuery) {
        deps.upsertRecentSearch(paletteQuery);
      }

      const selectedAction = actions[resolvedIndex];
      if (!selectedAction || typeof selectedAction.run !== 'function') {
        return;
      }

      close();
      await Promise.resolve(selectedAction.run());
    }

    return {
      isOpen,
      refresh,
      open,
      close,
      moveSelection,
      executeSelection
    };
  }

  function createGuideManager(deps) {
    const avatarMap = {
      intro: '/guide/mentor-custom.webp',
      explain: '/guide/mentor-custom.webp',
      focus: '/guide/mentor-custom.webp',
      smile: '/guide/mentor-custom.webp'
    };

    const steps = [
      {
        view: 'view-discovery',
        selector: null,
        title: 'Welcome To FedMDRM',
        body: 'FedMDRM is your single workspace for MDRM discovery, bookmark organization, report timeline review, ontology graph exploration, and file load operations. This quick guide will show the key places to navigate first.',
        intro: true,
        avatar: 'intro'
      },
      {
        view: 'view-discovery',
        selector: '#menu-discovery',
        title: 'Start In Discovery',
        body: 'Use this tab to search MDRM data first.',
        avatar: 'explain'
      },
      {
        view: 'view-discovery',
        selector: '#discoveryQueryInput',
        title: 'Type Your Query',
        body: 'Search by MDRM code, reporting form, or keyword.',
        avatar: 'focus'
      },
      {
        view: 'view-discovery',
        selector: '#commandPaletteBtn',
        title: 'Use Command Palette',
        body: 'Press Cmd/Ctrl + K to open quick actions. Type a report code, MDRM code, or any query and press Enter.',
        avatar: 'explain'
      },
      {
        view: 'view-bookmarks',
        selector: '#menu-bookmarks',
        title: 'Save To Bookmarks',
        body: 'Go here to organize saved items into groups.',
        avatar: 'smile'
      },
      {
        view: 'view-reporting',
        selector: '#menu-reporting',
        title: 'Open Reports',
        body: 'Review report history and drill into timeline changes.',
        avatar: 'focus'
      },
      {
        view: 'view-ontology',
        selector: '#menu-ontology',
        title: 'Open Ontology Graph',
        body: 'Use this tab to explore how Reports, MDRM, activity status, and item types relate to each other.',
        avatar: 'explain'
      },
      {
        view: 'view-ontology',
        selector: '#ontologyGraphHost',
        title: 'Work Directly From The Graph',
        body: 'Click nodes to scope selections and refresh summarized counts while detailed report and MDRM tables stay visible below.',
        avatar: 'focus'
      },
      {
        view: 'view-ontology',
        selector: '#ontologyNodeFilterPanel',
        title: 'Filter Visible Nodes',
        body: 'Use checkboxes to hide or show node groups, then use Reset Filter or Clear View when needed.',
        avatar: 'smile'
      },
      {
        view: 'view-load',
        selector: '#menu-load',
        title: 'Load New MDRM Files',
        body: 'Use this screen to upload and process fresh files.',
        avatar: 'explain'
      },
      {
        view: 'view-discovery',
        selector: '#runContextSelect',
        title: 'Set Time-Travel Context',
        body: 'Choose an "As of" run to time-travel. Discovery, reporting, bookmarks, and recent items are all shown for that selected context.',
        avatar: 'focus'
      },
      {
        view: 'view-discovery',
        selector: '#themeIconBtn',
        title: 'Switch Theme',
        body: 'Use this button to toggle between light and dark modes.',
        avatar: 'smile'
      },
      {
        view: 'view-discovery',
        selector: '#profileMenuBtn',
        title: 'Login And Profile',
        body: 'Use your profile menu for login, signup, and account actions.',
        avatar: 'smile'
      }
    ];

    let stepIndex = 0;
    let currentTarget = null;
    let closeTimer = null;
    let avatarTimer = null;

    function getSteps() {
      return steps.filter(step => step.view !== 'view-load' || deps.isAdminUser());
    }

    function setAvatar(avatarKey) {
      if (!deps.avatarImage) {
        return;
      }
      const nextSrc = avatarMap[avatarKey] || avatarMap.explain;
      if (deps.avatarImage.dataset.avatarSrc === nextSrc) {
        return;
      }
      if (avatarTimer) {
        window.clearTimeout(avatarTimer);
      }
      deps.avatarImage.classList.add('is-switching');
      avatarTimer = window.setTimeout(() => {
        deps.avatarImage.src = nextSrc;
        deps.avatarImage.dataset.avatarSrc = nextSrc;
        deps.avatarImage.classList.remove('is-switching');
      }, 120);
    }

    function clearTargetHighlight() {
      if (currentTarget) {
        currentTarget.classList.remove('guide-focus-target');
      }
      currentTarget = null;
    }

    function position() {
      if (!deps.overlay || deps.overlay.hidden || !deps.card || !deps.spotlight) {
        return;
      }

      if (!currentTarget) {
        const cardWidth = Math.min(640, window.innerWidth - 24);
        const cardHeight = deps.card.offsetHeight || 240;
        deps.card.style.width = cardWidth + 'px';
        deps.card.style.left = Math.max(12, (window.innerWidth - cardWidth) / 2) + 'px';
        deps.card.style.top = Math.max(12, (window.innerHeight - cardHeight) / 2) + 'px';
        deps.card.dataset.pointer = 'none';
        deps.spotlight.style.opacity = '0';
        return;
      }

      const rect = currentTarget.getBoundingClientRect();
      const pad = 8;
      const cardWidth = Math.min(480, window.innerWidth - 24);
      const cardHeight = deps.card.offsetHeight || 220;
      const left = Math.max(8, rect.left - pad);
      const top = Math.max(8, rect.top - pad);
      const width = Math.max(32, rect.width + pad * 2);
      const height = Math.max(32, rect.height + pad * 2);

      deps.spotlight.style.left = left + 'px';
      deps.spotlight.style.top = top + 'px';
      deps.spotlight.style.width = width + 'px';
      deps.spotlight.style.height = height + 'px';
      deps.card.style.width = cardWidth + 'px';

      let cardTop = rect.bottom + 16;
      let pointer = 'top';
      if (cardTop + cardHeight > window.innerHeight - 12) {
        cardTop = rect.top - cardHeight - 16;
        pointer = 'bottom';
      }
      if (cardTop < 12) {
        cardTop = 12;
      }

      let cardLeft = rect.left;
      if (cardLeft + cardWidth > window.innerWidth - 12) {
        cardLeft = window.innerWidth - cardWidth - 12;
      }
      if (cardLeft < 12) {
        cardLeft = 12;
      }

      deps.card.style.left = cardLeft + 'px';
      deps.card.style.top = cardTop + 'px';
      deps.card.dataset.pointer = pointer;
      deps.spotlight.style.opacity = '1';
      deps.card.style.setProperty(
        '--guide-pointer-x',
        Math.min(cardWidth - 24, Math.max(24, rect.left + rect.width / 2 - cardLeft)) + 'px'
      );
    }

    function renderStep() {
      if (!deps.overlay || deps.overlay.hidden) {
        return;
      }

      const visibleSteps = getSteps();
      if (!visibleSteps.length) {
        close(false);
        return;
      }

      stepIndex = Math.max(0, Math.min(stepIndex, visibleSteps.length - 1));
      const step = visibleSteps[stepIndex];
      deps.switchView(step.view);
      deps.closeProfileMenu();
      window.setTimeout(() => {
        const target = step.selector ? document.querySelector(step.selector) : null;
        clearTargetHighlight();
        currentTarget = target;
        if (currentTarget) {
          currentTarget.classList.add('guide-focus-target');
        }
        deps.stepLabel.textContent = 'Step ' + (stepIndex + 1) + ' of ' + visibleSteps.length;
        deps.titleEl.textContent = step.title;
        deps.bodyEl.textContent = step.body;
        setAvatar(step.avatar);
        deps.backBtn.disabled = stepIndex === 0;
        deps.nextBtn.textContent = stepIndex === visibleSteps.length - 1 ? 'Finish' : 'Next';
        position();
      }, 80);
    }

    function close(markSeen) {
      if (!deps.overlay) {
        return;
      }
      deps.overlay.classList.remove('is-open');
      clearTargetHighlight();
      if (closeTimer) {
        window.clearTimeout(closeTimer);
      }
      closeTimer = window.setTimeout(() => {
        deps.overlay.hidden = true;
      }, 240);
      if (markSeen !== false) {
        localStorage.setItem(deps.guideSeenKey, '1');
      }
    }

    function open(startIndex) {
      if (!deps.overlay) {
        return;
      }
      const visibleSteps = getSteps();
      if (!visibleSteps.length) {
        return;
      }
      stepIndex = Math.max(0, Math.min(startIndex || 0, visibleSteps.length - 1));
      if (closeTimer) {
        window.clearTimeout(closeTimer);
        closeTimer = null;
      }
      deps.overlay.hidden = false;
      window.requestAnimationFrame(() => deps.overlay.classList.add('is-open'));
      renderStep();
    }

    function next() {
      const visibleSteps = getSteps();
      if (stepIndex >= visibleSteps.length - 1) {
        close(true);
        return;
      }
      stepIndex += 1;
      renderStep();
    }

    function prev() {
      if (stepIndex <= 0) {
        return;
      }
      stepIndex -= 1;
      renderStep();
    }

    return {
      getSteps,
      position,
      open,
      close,
      next,
      prev
    };
  }

  window.ProjectAiUiModules = Object.assign(window.ProjectAiUiModules || {}, {
    createCommandPaletteManager,
    createGuideManager
  });
})();
