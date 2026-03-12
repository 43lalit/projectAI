(function () {
  function createAuthBookmarkManager(deps) {
    const el = deps.elements;

    function currentUser() {
      return deps.getCurrentUser();
    }

    function bookmarkGroups() {
      return deps.getBookmarkGroups();
    }

    function bookmarkItems() {
      return deps.getBookmarkItems();
    }

    function pendingBookmarkPayload() {
      return deps.getPendingBookmarkPayload();
    }

    function saveBookmarkModal() {
      return deps.getSaveBookmarkModal();
    }

    function setAuthStatus(text, css) {
      if (el.authStatusLabel) {
        el.authStatusLabel.textContent = text;
        el.authStatusLabel.className = 'status ' + css + ' mt-2';
      }
    }

    function buildBookmarkPayload(itemKey, title, subtitle, url, itemType) {
      return {
        groupId: null,
        itemType: itemType || 'mdrm',
        itemKey: String(itemKey || '').trim(),
        title: String(title || itemKey || 'Item').trim(),
        subtitle: String(subtitle || '').trim(),
        url: String(url || '').trim()
      };
    }

    function showSaveBookmarkModal(payload) {
      deps.setPendingBookmarkPayload(payload);
      if (el.saveBookmarkContext) {
        el.saveBookmarkContext.textContent = payload.itemKey + (payload.subtitle ? ' • ' + payload.subtitle : '');
      }
      if (el.saveBookmarkGroupSelect) {
        el.saveBookmarkGroupSelect.value = '';
      }
      if (el.saveBookmarkNewGroupInput) {
        el.saveBookmarkNewGroupInput.value = '';
      }
      if (el.saveBookmarkNewGroupWrap) {
        el.saveBookmarkNewGroupWrap.style.display = 'none';
      }
      if (!saveBookmarkModal() && el.saveBookmarkModalEl && window.bootstrap) {
        deps.setSaveBookmarkModal(new window.bootstrap.Modal(el.saveBookmarkModalEl));
      }
      if (saveBookmarkModal()) {
        saveBookmarkModal().show();
      } else {
        savePendingBookmark();
      }
    }

    function renderBookmarkGroups() {
      if (!currentUser()) {
        if (el.bookmarkGroupsContainer) {
          el.bookmarkGroupsContainer.innerHTML = '<div class="bookmark-empty">Login to create collections.</div>';
        }
        if (el.bookmarkFilterGroupSelect) {
          el.bookmarkFilterGroupSelect.innerHTML = '<option value="">All collections</option>';
        }
        if (el.saveBookmarkGroupSelect) {
          el.saveBookmarkGroupSelect.innerHTML = '<option value="">Default Favorites</option>';
        }
        return;
      }

      const groups = Array.isArray(bookmarkGroups()) ? bookmarkGroups() : [];
      const selectedFilterGroup = el.bookmarkFilterGroupSelect ? el.bookmarkFilterGroupSelect.value : '';
      if (el.bookmarkFilterGroupSelect) {
        el.bookmarkFilterGroupSelect.innerHTML = '<option value="">All collections</option>' + groups.map(group =>
          '<option value="' + group.groupId + '">' + deps.escapeHtml(group.name) + ' (' + group.itemCount + ')</option>'
        ).join('');
      }
      if (el.saveBookmarkGroupSelect) {
        const selectedSaveGroup = el.saveBookmarkGroupSelect.value;
        el.saveBookmarkGroupSelect.innerHTML = '<option value="">Default Favorites</option>' + groups.map(group =>
          '<option value="' + group.groupId + '">' + deps.escapeHtml(group.name) + '</option>'
        ).join('') + '<option value="__new__">+ Create new group</option>';
        if (selectedSaveGroup && el.saveBookmarkGroupSelect.querySelector('option[value="' + selectedSaveGroup + '"]')) {
          el.saveBookmarkGroupSelect.value = selectedSaveGroup;
        }
      }
      if (selectedFilterGroup && el.bookmarkFilterGroupSelect) {
        el.bookmarkFilterGroupSelect.value = selectedFilterGroup;
      }

      if (!groups.length) {
        if (el.bookmarkGroupsContainer) {
          el.bookmarkGroupsContainer.innerHTML = '<div class="bookmark-empty">No collections yet. Create one to organize bookmarks.</div>';
        }
        return;
      }

      if (el.bookmarkGroupsContainer) {
        el.bookmarkGroupsContainer.innerHTML = (
          '<div class="bookmark-group-stack">' +
            '<button class="bookmark-group-chip ' + (!selectedFilterGroup ? 'active' : '') + '" type="button" data-group-select="">' +
              '<span class="bookmark-group-name">All Collections</span>' +
              '<span class="bookmark-group-count">' + groups.reduce((sum, group) => sum + (Number(group.itemCount) || 0), 0) + '</span>' +
            '</button>' +
            groups.map(group => (
              '<button class="bookmark-group-chip ' + (String(selectedFilterGroup) === String(group.groupId) ? 'active' : '') + '" type="button" data-group-select="' + group.groupId + '">' +
                '<span class="bookmark-group-name">' + deps.escapeHtml(group.name) + '</span>' +
                '<span class="bookmark-group-count">' + group.itemCount + '</span>' +
              '</button>'
            )).join('') +
          '</div>'
        );
      }
    }

    function getVisibleBookmarkItems() {
      const query = String(el.bookmarkSearchInput?.value || '').trim().toLowerCase();
      const typeFilter = String(el.bookmarkTypeFilterSelect?.value || 'all').toLowerCase();
      const sortOrder = String(el.bookmarkSortSelect?.value || 'newest').toLowerCase();
      let items = Array.isArray(bookmarkItems()) ? [...bookmarkItems()] : [];

      if (typeFilter !== 'all') {
        items = items.filter(item => String(item.itemType || '').trim().toLowerCase() === typeFilter);
      }
      if (query) {
        items = items.filter(item => {
          const haystack = [item.itemKey, item.title, item.subtitle, item.groupName, item.itemType]
            .map(value => String(value || '').toLowerCase())
            .join(' ');
          return haystack.includes(query);
        });
      }

      if (sortOrder === 'az') {
        items.sort((a, b) => String(a.title || a.itemKey || '').localeCompare(String(b.title || b.itemKey || ''), undefined, { sensitivity: 'base' }));
      } else if (sortOrder === 'oldest') {
        items.sort((a, b) => Number(a.createdAt || 0) - Number(b.createdAt || 0));
      } else {
        items.sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
      }
      return items;
    }

    function renderBookmarkItems() {
      if (!currentUser()) {
        if (el.bookmarkItemsContainer) {
          el.bookmarkItemsContainer.innerHTML = '<div class="bookmark-empty bookmark-empty-main">Login to browse bookmarks.</div>';
        }
        return;
      }

      const visibleItems = getVisibleBookmarkItems();
      if (!visibleItems.length) {
        const hasAnyBookmarks = Array.isArray(bookmarkItems()) && bookmarkItems().length > 0;
        if (el.bookmarkItemsContainer) {
          el.bookmarkItemsContainer.innerHTML = hasAnyBookmarks
            ? '<div class="bookmark-empty bookmark-empty-main">No bookmarks match the current filters.</div>'
            : '<div class="bookmark-empty bookmark-empty-main">No bookmarks yet. Save items from Discovery to build your library.</div>';
        }
        return;
      }

      if (el.bookmarkItemsContainer) {
        el.bookmarkItemsContainer.innerHTML = (
          '<div class="bookmark-list">' +
            visibleItems.map(item => (
              '<div class="bookmark-item">' +
                '<div class="bookmark-item-head">' +
                  '<div class="bookmark-item-kicker">' + deps.escapeHtml(String(item.itemType || 'item').toUpperCase()) + '</div>' +
                  '<div class="bookmark-item-time">' + deps.formatRunDate(item.createdAt) + '</div>' +
                '</div>' +
                '<div class="bookmark-title">' +
                  (item.url
                    ? '<a href="' + deps.escapeHtml(item.url) + '" title="Open bookmark">' + deps.escapeHtml(item.itemKey || item.title || 'Item') + '</a>'
                    : deps.escapeHtml(item.itemKey || item.title || 'Item')) +
                '</div>' +
                '<div class="bookmark-subtitle">' + deps.escapeHtml(item.title || '') + (item.groupName ? ' • ' + deps.escapeHtml(item.groupName) : '') + '</div>' +
                (item.subtitle ? '<div class="bookmark-note">' + deps.escapeHtml(item.subtitle) + '</div>' : '') +
                '<div class="bookmark-actions-row">' +
                  '<select class="form-select form-select-sm bookmark-move-group" data-bookmark-id="' + item.bookmarkId + '">' +
                    '<option value="">No group</option>' +
                    (bookmarkGroups() || []).map(group => (
                      '<option value="' + group.groupId + '" ' + (item.groupId === group.groupId ? 'selected' : '') + '>' + deps.escapeHtml(group.name) + '</option>'
                    )).join('') +
                  '</select>' +
                  '<button class="btn btn-sm btn-outline-secondary move-bookmark-btn" type="button" data-bookmark-id="' + item.bookmarkId + '">Move</button>' +
                  (item.url ? '<a class="btn btn-sm btn-outline-primary" href="' + deps.escapeHtml(item.url) + '">Open</a>' : '') +
                  '<button class="btn btn-sm btn-outline-danger remove-bookmark-btn" type="button" data-bookmark-id="' + item.bookmarkId + '">Remove</button>' +
                '</div>' +
              '</div>'
            )).join('') +
          '</div>'
        );
      }
    }

    function isBookmarkedByTypeAndKey(itemType, itemKey) {
      const targetType = String(itemType || '').trim().toLowerCase();
      const targetKey = String(itemKey || '').trim().toLowerCase();
      if (!targetType || !targetKey) {
        return false;
      }
      return (bookmarkItems() || []).some(bookmark =>
        String(bookmark.itemType || '').trim().toLowerCase() === targetType
        && String(bookmark.itemKey || '').trim().toLowerCase() === targetKey
      );
    }

    function findBookmarkIdByTypeAndKey(itemType, itemKey) {
      const targetType = String(itemType || '').trim().toLowerCase();
      const targetKey = String(itemKey || '').trim().toLowerCase();
      if (!targetType || !targetKey) {
        return null;
      }
      const bookmark = (bookmarkItems() || []).find(entry =>
        String(entry.itemType || '').trim().toLowerCase() === targetType
        && String(entry.itemKey || '').trim().toLowerCase() === targetKey
      );
      return bookmark ? Number(bookmark.bookmarkId) : null;
    }

    async function removeBookmark(bookmarkId) {
      if (!currentUser()) {
        return;
      }
      try {
        const response = await deps.apiFetch(deps.appendRunContext('/api/bookmarks/items/' + bookmarkId), { method: 'DELETE' });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        await refreshBookmarks();
      } catch (_) {
        setAuthStatus('Could not remove bookmark.', 'err');
      }
    }

    function removeBookmarkByTypeAndKey(itemType, itemKey) {
      const bookmarkId = findBookmarkIdByTypeAndKey(itemType, itemKey);
      if (!bookmarkId) {
        return Promise.resolve();
      }
      return removeBookmark(bookmarkId);
    }

    async function removeBookmarkByItemKey(itemKey) {
      const key = String(itemKey || '').trim().toLowerCase();
      if (!key || !currentUser()) {
        return;
      }
      await removeBookmarkByTypeAndKey('mdrm', key);
    }

    function isSelectedReportBookmarked() {
      const report = String(deps.getSelectedReportingForm() || '').trim();
      return report ? isBookmarkedByTypeAndKey('report', report) : false;
    }

    function updateReportBookmarkButton() {
      if (!el.bookmarkReportBtn) {
        return;
      }
      const hasReport = !!(deps.getSelectedReportingForm() && String(deps.getSelectedReportingForm()).trim());
      const bookmarked = hasReport && isSelectedReportBookmarked();
      el.bookmarkReportBtn.disabled = !hasReport;
      el.bookmarkReportBtn.classList.toggle('btn-warning', bookmarked);
      el.bookmarkReportBtn.classList.toggle('btn-outline-warning', !bookmarked);
      if (el.bookmarkReportBtnIcon) {
        el.bookmarkReportBtnIcon.textContent = bookmarked ? '★' : '☆';
      }
      el.bookmarkReportBtn.title = bookmarked ? 'Remove bookmark' : 'Bookmark report';
      el.bookmarkReportBtn.setAttribute('aria-label', bookmarked ? 'Remove report bookmark' : 'Bookmark report');
    }

    function refreshReportingBookmarkFlags() {
      updateReportBookmarkButton();
      const activeGridApi = deps.getActiveMdrmGridApi();
      if (activeGridApi) {
        const rows = [];
        activeGridApi.forEachNode(node => {
          const code = String(node.data && node.data.mdrmCode ? node.data.mdrmCode : '').trim();
          rows.push({
            ...(node.data || {}),
            bookmarked: isBookmarkedByTypeAndKey('mdrm', code)
          });
        });
        activeGridApi.setGridOption('rowData', rows);
      }
      const netChangeGridApi = deps.getNetChangeGridApi();
      if (netChangeGridApi) {
        const rows = [];
        netChangeGridApi.forEachNode(node => {
          const code = String(node.data && node.data.mdrmCode ? node.data.mdrmCode : '').trim();
          rows.push({
            ...(node.data || {}),
            bookmarked: isBookmarkedByTypeAndKey('mdrm', code)
          });
        });
        netChangeGridApi.setGridOption('rowData', rows);
      }
    }

    function isRecentItemBookmarked(item) {
      if (!item) {
        return false;
      }
      return isBookmarkedByTypeAndKey(item.type, item.key);
    }

    function findBookmarkIdForRecentItem(item) {
      if (!item) {
        return null;
      }
      return findBookmarkIdByTypeAndKey(item.type, item.key);
    }

    async function toggleRecentItemBookmark(index) {
      const item = (deps.getRecentItems() || [])[index];
      if (!item) {
        return;
      }
      if (!currentUser()) {
        setAuthStatus('Login required to save bookmarks.', 'err');
        if (el.profileMenu && el.profileMenuBtn) {
          el.profileMenu.classList.add('open');
          el.profileMenuBtn.setAttribute('aria-expanded', 'true');
        }
        return;
      }
      const existingBookmarkId = findBookmarkIdForRecentItem(item);
      if (existingBookmarkId) {
        await removeBookmark(existingBookmarkId);
        deps.renderRecentItems();
        return;
      }
      const response = await deps.apiFetch(deps.appendRunContext('/api/bookmarks/items'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          groupId: null,
          itemType: item.type,
          itemKey: item.key,
          title: item.label,
          subtitle: item.subtitle || '',
          url: item.url || ''
        })
      });
      if (!response.ok) {
        throw new Error('HTTP ' + response.status);
      }
      await refreshBookmarks();
      deps.renderRecentItems();
    }

    function syncAuthUi() {
      const isLoggedIn = !!currentUser();
      el.authLoginBtn.disabled = isLoggedIn;
      el.authRegisterBtn.disabled = isLoggedIn;
      el.authResetBtn.disabled = isLoggedIn;
      el.authLogoutBtn.disabled = !isLoggedIn;
      el.createGroupBtn.disabled = !isLoggedIn;
      el.bookmarkFilterGroupSelect.disabled = !isLoggedIn;
      if (el.bookmarkSearchInput) el.bookmarkSearchInput.disabled = !isLoggedIn;
      if (el.bookmarkTypeFilterSelect) el.bookmarkTypeFilterSelect.disabled = !isLoggedIn;
      if (el.bookmarkSortSelect) el.bookmarkSortSelect.disabled = !isLoggedIn;
      el.bookmarkUserLabel.textContent = isLoggedIn ? 'Logged in as ' + currentUser().displayName : 'Login required';
      el.profileNameLabel.textContent = isLoggedIn ? currentUser().displayName : 'Guest';
      if (el.authInputRows && el.authLoginCol && el.authRegisterCol && el.authLogoutCol) {
        if (el.authModeRow) el.authModeRow.style.display = isLoggedIn ? 'none' : '';
        if (isLoggedIn) {
          if (el.authEmailCol) el.authEmailCol.style.display = 'none';
          if (el.authFirstNameCol) el.authFirstNameCol.style.display = 'none';
          if (el.authLastNameCol) el.authLastNameCol.style.display = 'none';
          if (el.authPasswordCol) el.authPasswordCol.style.display = 'none';
          if (el.authConfirmPasswordCol) el.authConfirmPasswordCol.style.display = 'none';
          if (el.authShowPasswordCol) el.authShowPasswordCol.style.display = 'none';
          el.authLoginCol.style.display = 'none';
          el.authRegisterCol.style.display = 'none';
          if (el.authResetCol) el.authResetCol.style.display = 'none';
        } else {
          deps.setAuthMode(deps.getAuthMode());
        }
        el.authLogoutCol.style.display = '';
      }
      renderBookmarkGroups();
      renderBookmarkItems();
      deps.renderRecentItems();
      el.renameGroupBtn.disabled = !isLoggedIn || !el.bookmarkFilterGroupSelect.value;
      el.deleteGroupBtn.disabled = !isLoggedIn || !el.bookmarkFilterGroupSelect.value;
      deps.syncAdminLoadAccess();
      deps.syncReportMetadataRefreshAccess();
      if (deps.isOntologyViewActive()) {
        deps.loadOntologyGraph(true).catch(() => {});
      }
    }

    async function refreshBookmarks() {
      if (!currentUser()) {
        return;
      }
      try {
        const filterGroupId = el.bookmarkFilterGroupSelect.value ? Number(el.bookmarkFilterGroupSelect.value) : null;
        const itemsUrl = filterGroupId ? '/api/bookmarks/items?groupId=' + encodeURIComponent(filterGroupId) : '/api/bookmarks/items';
        const [groupsRes, itemsRes] = await Promise.all([
          fetch(deps.appendRunContext('/api/bookmarks/groups')),
          fetch(deps.appendRunContext(itemsUrl))
        ]);
        if (!groupsRes.ok || !itemsRes.ok) {
          throw new Error('Failed to load bookmarks');
        }
        deps.setBookmarkGroups(await groupsRes.json());
        deps.setBookmarkItems(await itemsRes.json());
      } catch (_) {
        deps.setBookmarkGroups([]);
        deps.setBookmarkItems([]);
      }
      deps.refreshDiscoveryBookmarkFlags();
      syncAuthUi();
    }

    async function refreshAuthState() {
      try {
        const response = await fetch('/api/auth/me');
        const payload = await response.json();
        deps.setCurrentUser(payload && payload.authenticated ? payload.user : null);
      } catch (_) {
        deps.setCurrentUser(null);
      }
      if (currentUser()) {
        setAuthStatus('Logged in (' + (currentUser().role || 'USER') + ')', 'ok');
        if (el.authStatusLabel) {
          el.authStatusLabel.title = currentUser().displayName + ' • ' + currentUser().email;
        }
        await refreshBookmarks();
        await deps.loadRecentItems();
      } else {
        setAuthStatus('Not logged in', 'neutral');
        if (el.authStatusLabel) {
          el.authStatusLabel.removeAttribute('title');
        }
        deps.setBookmarkGroups([]);
        deps.setBookmarkItems([]);
        deps.refreshDiscoveryBookmarkFlags();
        deps.loadRecentItemsFromLocal();
        deps.renderRecentItems();
      }
      syncAuthUi();
    }

    async function login() {
      const email = String(el.authEmailInput.value || '').trim();
      const password = String(el.authPasswordInput.value || '').trim();
      if (!email || !password) {
        setAuthStatus('Email and password are required.', 'err');
        return;
      }
      try {
        const response = await deps.apiFetch('/api/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password })
        });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        el.authPasswordInput.value = '';
        await refreshAuthState();
      } catch (_) {
        setAuthStatus('Login failed. Check credentials.', 'err');
      }
    }

    async function register() {
      const firstName = String(el.authFirstNameInput.value || '').trim();
      const lastName = String(el.authLastNameInput.value || '').trim();
      const email = String(el.authEmailInput.value || '').trim();
      const password = String(el.authPasswordInput.value || '').trim();
      const confirmPassword = String(el.authConfirmPasswordInput.value || '').trim();
      if (!firstName || !lastName || !email || !password || !confirmPassword) {
        setAuthStatus('First name, last name, email and password are required.', 'err');
        return;
      }
      if (password !== confirmPassword) {
        setAuthStatus('Passwords do not match.', 'err');
        return;
      }
      try {
        const response = await deps.apiFetch('/api/auth/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ firstName, lastName, email, password })
        });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        el.authPasswordInput.value = '';
        el.authConfirmPasswordInput.value = '';
        await refreshAuthState();
      } catch (_) {
        setAuthStatus('Registration failed. Email may already exist.', 'err');
      }
    }

    async function resetPassword() {
      const email = String(el.authEmailInput.value || '').trim();
      const newPassword = String(el.authPasswordInput.value || '').trim();
      const confirmPassword = String(el.authConfirmPasswordInput.value || '').trim();
      if (!email || !newPassword || !confirmPassword) {
        setAuthStatus('Email, new password and confirmation are required.', 'err');
        return;
      }
      if (newPassword !== confirmPassword) {
        setAuthStatus('Passwords do not match.', 'err');
        return;
      }
      try {
        const response = await deps.apiFetch('/api/auth/reset-password', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, newPassword })
        });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        el.authPasswordInput.value = '';
        el.authConfirmPasswordInput.value = '';
        deps.setAuthMode('login');
        setAuthStatus('Password reset successful. Please login.', 'ok');
      } catch (_) {
        setAuthStatus('Password reset failed. Check email and try again.', 'err');
      }
    }

    async function logout() {
      try {
        await deps.apiFetch('/api/auth/logout', { method: 'POST' });
      } catch (_) {
      }
      deps.setCurrentUser(null);
      deps.setBookmarkGroups([]);
      deps.setBookmarkItems([]);
      el.authPasswordInput.value = '';
      el.authConfirmPasswordInput.value = '';
      if (el.authShowPasswordInput) {
        el.authShowPasswordInput.checked = false;
      }
      deps.setPasswordVisibility(false);
      deps.setAuthMode('');
      syncAuthUi();
      setAuthStatus('Logged out', 'neutral');
      deps.loadRecentItemsFromLocal();
      deps.renderRecentItems();
    }

    function submitAuthWithEnter() {
      if (currentUser()) {
        logout();
        return;
      }
      if (deps.getAuthMode() === 'register') {
        register();
        return;
      }
      if (deps.getAuthMode() === 'reset') {
        resetPassword();
        return;
      }
      login();
    }

    async function createGroup() {
      const name = String(el.newGroupNameInput.value || '').trim();
      if (!name) {
        return;
      }
      await createGroupByName(name);
      el.newGroupNameInput.value = '';
    }

    async function createGroupByName(name) {
      const groupName = String(name || '').trim();
      if (!groupName) {
        return null;
      }
      try {
        const response = await deps.apiFetch(deps.appendRunContext('/api/bookmarks/groups'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: groupName })
        });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        await refreshBookmarks();
        const created = (bookmarkGroups() || []).find(group => String(group.name || '').toLowerCase() === groupName.toLowerCase());
        return created ? Number(created.groupId) : null;
      } catch (_) {
        setAuthStatus('Could not create group.', 'err');
        return null;
      }
    }

    async function addBookmarkFromButton(button) {
      if (!currentUser()) {
        setAuthStatus('Login required to save bookmarks.', 'err');
        return;
      }
      const payload = buildBookmarkPayload(
        button.dataset.itemKey || '',
        button.dataset.title || button.dataset.itemKey || 'Item',
        button.dataset.subtitle || '',
        button.dataset.url || '',
        button.dataset.itemType || 'mdrm'
      );
      if (!payload.itemKey) {
        return;
      }
      showSaveBookmarkModal(payload);
    }

    async function savePendingBookmark() {
      if (!pendingBookmarkPayload()) {
        return;
      }
      let targetGroupId = null;
      if (el.saveBookmarkGroupSelect) {
        const selected = el.saveBookmarkGroupSelect.value;
        if (selected === '__new__') {
          const groupName = String(el.saveBookmarkNewGroupInput.value || '').trim();
          if (!groupName) {
            setAuthStatus('Enter a group name to create and save.', 'err');
            return;
          }
          const createdGroupId = await createGroupByName(groupName);
          if (!createdGroupId) {
            return;
          }
          targetGroupId = createdGroupId;
        } else if (selected) {
          targetGroupId = Number(selected);
        }
      }

      const payload = {
        ...pendingBookmarkPayload(),
        groupId: Number.isNaN(targetGroupId) ? null : targetGroupId
      };
      try {
        const response = await deps.apiFetch(deps.appendRunContext('/api/bookmarks/items'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        await refreshBookmarks();
        setAuthStatus('Bookmark saved.', 'ok');
        if (saveBookmarkModal()) {
          saveBookmarkModal().hide();
        }
        deps.setPendingBookmarkPayload(null);
      } catch (_) {
        setAuthStatus('Could not save bookmark. It may already exist.', 'err');
      }
    }

    async function renameSelectedGroup() {
      const groupId = el.bookmarkFilterGroupSelect.value ? Number(el.bookmarkFilterGroupSelect.value) : null;
      const name = String(el.renameGroupNameInput.value || '').trim();
      if (!groupId || !name) {
        return;
      }
      try {
        const response = await deps.apiFetch(deps.appendRunContext('/api/bookmarks/groups/' + groupId), {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name })
        });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        el.renameGroupNameInput.value = '';
        await refreshBookmarks();
      } catch (_) {
        setAuthStatus('Could not rename group.', 'err');
      }
    }

    async function deleteSelectedGroup() {
      const groupId = el.bookmarkFilterGroupSelect.value ? Number(el.bookmarkFilterGroupSelect.value) : null;
      if (!groupId) {
        return;
      }
      try {
        const response = await deps.apiFetch(deps.appendRunContext('/api/bookmarks/groups/' + groupId), { method: 'DELETE' });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        el.bookmarkFilterGroupSelect.value = '';
        await refreshBookmarks();
      } catch (_) {
        setAuthStatus('Could not delete group.', 'err');
      }
    }

    async function moveBookmark(bookmarkId, groupId) {
      try {
        const response = await deps.apiFetch(deps.appendRunContext('/api/bookmarks/items/' + bookmarkId + '/group'), {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ groupId })
        });
        if (!response.ok) {
          throw new Error('HTTP ' + response.status);
        }
        await refreshBookmarks();
      } catch (_) {
        setAuthStatus('Could not move bookmark.', 'err');
      }
    }

    return {
      setAuthStatus,
      buildBookmarkPayload,
      showSaveBookmarkModal,
      renderBookmarkGroups,
      getVisibleBookmarkItems,
      renderBookmarkItems,
      isBookmarkedByTypeAndKey,
      findBookmarkIdByTypeAndKey,
      removeBookmarkByTypeAndKey,
      isSelectedReportBookmarked,
      updateReportBookmarkButton,
      refreshReportingBookmarkFlags,
      isRecentItemBookmarked,
      findBookmarkIdForRecentItem,
      toggleRecentItemBookmark,
      syncAuthUi,
      refreshAuthState,
      refreshBookmarks,
      login,
      register,
      resetPassword,
      logout,
      submitAuthWithEnter,
      createGroup,
      createGroupByName,
      addBookmarkFromButton,
      savePendingBookmark,
      removeBookmark,
      removeBookmarkByItemKey,
      renameSelectedGroup,
      deleteSelectedGroup,
      moveBookmark
    };
  }

  window.ProjectAiUiModules = Object.assign(window.ProjectAiUiModules || {}, {
    createAuthBookmarkManager
  });
})();
