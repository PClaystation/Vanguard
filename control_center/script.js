const PRESETS = {
  off: {
    guard_enabled: false,
  },
  relaxed: {
    guard_enabled: true,
    guard_threshold: 12,
    guard_window_seconds: 30,
    guard_new_account_hours: 12,
    guard_slowmode_seconds: 20,
    guard_cooldown_seconds: 240,
    guard_slowmode_scope: "trigger",
    guard_timeout_seconds: 0,
    guard_join_threshold: 9,
    guard_join_window_seconds: 45,
    guard_mention_per_message: 10,
    guard_mention_burst_threshold: 4,
    guard_mention_window_seconds: 20,
    guard_duplicate_threshold: 5,
    guard_duplicate_window_seconds: 25,
    guard_link_threshold: 7,
    guard_link_window_seconds: 45,
  },
  balanced: {
    guard_enabled: true,
    guard_threshold: 8,
    guard_window_seconds: 30,
    guard_new_account_hours: 24,
    guard_slowmode_seconds: 30,
    guard_cooldown_seconds: 300,
    guard_slowmode_scope: "active",
    guard_max_slowmode_channels: 3,
    guard_critical_slowmode_seconds: 120,
    guard_timeout_seconds: 300,
    guard_delete_trigger_message: false,
    guard_join_threshold: 6,
    guard_join_window_seconds: 45,
    guard_mention_per_message: 6,
    guard_mention_burst_threshold: 3,
    guard_mention_window_seconds: 20,
    guard_duplicate_threshold: 4,
    guard_duplicate_window_seconds: 25,
    guard_duplicate_min_chars: 12,
    guard_link_threshold: 5,
    guard_link_window_seconds: 45,
    guard_detect_joins: true,
    guard_detect_mentions: true,
    guard_detect_duplicates: true,
    guard_detect_links: true,
  },
  strict: {
    guard_enabled: true,
    guard_threshold: 6,
    guard_window_seconds: 20,
    guard_new_account_hours: 48,
    guard_slowmode_seconds: 45,
    guard_cooldown_seconds: 180,
    guard_slowmode_scope: "active",
    guard_max_slowmode_channels: 6,
    guard_critical_slowmode_seconds: 180,
    guard_timeout_seconds: 900,
    guard_delete_trigger_message: true,
    guard_join_threshold: 5,
    guard_join_window_seconds: 35,
    guard_mention_per_message: 4,
    guard_mention_burst_threshold: 2,
    guard_mention_window_seconds: 20,
    guard_duplicate_threshold: 3,
    guard_duplicate_window_seconds: 20,
    guard_duplicate_min_chars: 10,
    guard_link_threshold: 4,
    guard_link_window_seconds: 35,
    guard_detect_joins: true,
    guard_detect_mentions: true,
    guard_detect_duplicates: true,
    guard_detect_links: true,
  },
  siege: {
    guard_enabled: true,
    guard_threshold: 4,
    guard_window_seconds: 15,
    guard_new_account_hours: 72,
    guard_slowmode_seconds: 60,
    guard_cooldown_seconds: 120,
    guard_slowmode_scope: "active",
    guard_max_slowmode_channels: 12,
    guard_critical_slowmode_seconds: 300,
    guard_timeout_seconds: 1800,
    guard_delete_trigger_message: true,
    guard_join_threshold: 4,
    guard_join_window_seconds: 25,
    guard_mention_per_message: 3,
    guard_mention_burst_threshold: 2,
    guard_mention_window_seconds: 15,
    guard_duplicate_threshold: 3,
    guard_duplicate_window_seconds: 15,
    guard_duplicate_min_chars: 8,
    guard_link_threshold: 3,
    guard_link_window_seconds: 25,
    guard_detect_joins: true,
    guard_detect_mentions: true,
    guard_detect_duplicates: true,
    guard_detect_links: true,
  },
};

const CONTROL_BASE = "/control";
const API_BASE = `${CONTROL_BASE}/api`;
const AUTH_BASE = `${CONTROL_BASE}/auth`;
const POPUP_NAME = "continental-id-login";
const POPUP_FEATURES = "popup=yes,width=520,height=760";
const UI_GUARD_DEFAULTS = {
  guard_enabled: false,
  guard_threshold: 8,
  guard_window_seconds: 30,
  guard_new_account_hours: 24,
  guard_slowmode_seconds: 30,
  guard_cooldown_seconds: 300,
  guard_slowmode_scope: "trigger",
  guard_max_slowmode_channels: 3,
  guard_critical_slowmode_seconds: 120,
  guard_timeout_seconds: 0,
  guard_delete_trigger_message: false,
  guard_join_threshold: 6,
  guard_join_window_seconds: 45,
  guard_mention_per_message: 6,
  guard_mention_burst_threshold: 3,
  guard_mention_window_seconds: 20,
  guard_duplicate_threshold: 4,
  guard_duplicate_window_seconds: 25,
  guard_duplicate_min_chars: 12,
  guard_link_threshold: 5,
  guard_link_window_seconds: 45,
  guard_detect_joins: true,
  guard_detect_mentions: true,
  guard_detect_duplicates: true,
  guard_detect_links: true,
};

const state = {
  authenticated: false,
  mode: null,
  user: null,
  continental: null,
  license: null,
  continentalLoginUrl: "",
  continentalDashboardUrl: "",
  continentalAuthEnabled: false,
  guilds: [],
  selectedGuildId: null,
  detail: null,
  guildRequestToken: 0,
};

const fieldIds = {
  welcome_channel_id: "welcome-channel",
  welcome_role_id: "welcome-role",
  welcome_message: "welcome-message",
  ops_channel_id: "ops-channel",
  log_channel_id: "log-channel",
  lockdown_role_id: "lockdown-role",
};

const guardNumberFields = [
  ["guard_threshold", "guard-threshold"],
  ["guard_window_seconds", "guard-window-seconds"],
  ["guard_new_account_hours", "guard-new-account-hours"],
  ["guard_slowmode_seconds", "guard-slowmode-seconds"],
  ["guard_cooldown_seconds", "guard-cooldown-seconds"],
  ["guard_max_slowmode_channels", "guard-max-slowmode-channels"],
  ["guard_critical_slowmode_seconds", "guard-critical-slowmode-seconds"],
  ["guard_timeout_seconds", "guard-timeout-seconds"],
  ["guard_join_threshold", "guard-join-threshold"],
  ["guard_join_window_seconds", "guard-join-window-seconds"],
  ["guard_mention_per_message", "guard-mention-per-message"],
  ["guard_mention_burst_threshold", "guard-mention-burst-threshold"],
  ["guard_mention_window_seconds", "guard-mention-window-seconds"],
  ["guard_duplicate_threshold", "guard-duplicate-threshold"],
  ["guard_duplicate_window_seconds", "guard-duplicate-window-seconds"],
  ["guard_duplicate_min_chars", "guard-duplicate-min-chars"],
  ["guard_link_threshold", "guard-link-threshold"],
  ["guard_link_window_seconds", "guard-link-window-seconds"],
];

const guardCheckboxFields = [
  ["guard_enabled", "guard-enabled"],
  ["guard_delete_trigger_message", "guard-delete-trigger-message"],
  ["guard_detect_joins", "guard-detect-joins"],
  ["guard_detect_mentions", "guard-detect-mentions"],
  ["guard_detect_duplicates", "guard-detect-duplicates"],
  ["guard_detect_links", "guard-detect-links"],
];

const authStatus = document.querySelector("#auth-status");
const continentalLogin = document.querySelector("#continental-login");
const logoutButton = document.querySelector("#logout-button");
const refreshButton = document.querySelector("#refresh-guilds");
const guildList = document.querySelector("#guild-list");
const emptyState = document.querySelector("#empty-state");
const dashboard = document.querySelector("#dashboard");
const settingsForm = document.querySelector("#settings-form");
const resetButton = document.querySelector("#reset-form");
const guardPresetSelect = document.querySelector("#guard-preset-select");
const presetHint = document.querySelector("#preset-hint");
const toast = document.querySelector("#toast");
const continentalBadge = document.querySelector("#continental-badge");
const continentalDetail = document.querySelector("#continental-detail");
const licenseBadge = document.querySelector("#license-badge");
const licenseDetail = document.querySelector("#license-detail");
const guildAccessBadge = document.querySelector("#guild-access-badge");
const dashboardNotice = document.querySelector("#dashboard-notice");
const lockdownEnableButton = document.querySelector("#lockdown-enable");
const lockdownDisableButton = document.querySelector("#lockdown-disable");
const lockdownTargetDetail = document.querySelector("#lockdown-target-detail");

bootstrap();

async function bootstrap() {
  await loadSession();
  if (state.authenticated) {
    await loadGuilds();
  }
}

continentalLogin.addEventListener("click", async () => {
  const session = await loadSession();
  if (!state.continentalAuthEnabled || !state.continentalLoginUrl) {
    showToast("Continental ID sign-in is not configured on this Vanguard instance.", "error");
    return;
  }
  if (session?.authenticated) {
    await loadGuilds();
    return;
  }

  const popupUrl = buildContinentalPopupUrl(state.continentalLoginUrl);
  const popup = window.open(popupUrl, POPUP_NAME, POPUP_FEATURES);
  if (!popup) {
    showToast("The Continental ID sign-in popup was blocked by your browser.", "error");
    return;
  }
  popup.focus();
});

logoutButton.addEventListener("click", async () => {
  try {
    await fetch(`${AUTH_BASE}/logout`, {
      method: "POST",
      credentials: "same-origin",
    });
  } catch (error) {
    showToast("Failed to log out cleanly.", "error");
  }
  state.authenticated = false;
  state.user = null;
  state.mode = null;
  state.continental = null;
  state.guilds = [];
  state.selectedGuildId = null;
  state.detail = null;
  await loadSession();
  renderGuildList();
  dashboard.classList.add("hidden");
  emptyState.classList.remove("hidden");
  renderDashboardNotice(null);
});

refreshButton.addEventListener("click", async () => {
  await loadGuilds();
});

lockdownEnableButton.addEventListener("click", async () => {
  await runLockdownAction(true);
});

lockdownDisableButton.addEventListener("click", async () => {
  await runLockdownAction(false);
});

resetButton.addEventListener("click", () => {
  if (state.detail) {
    fillForm(state.detail);
    showToast("Reset unsaved changes.", "success");
  }
});

guardPresetSelect.addEventListener("change", () => {
  const selectedPreset = guardPresetSelect.value;
  if (selectedPreset === "custom" || !PRESETS[selectedPreset]) {
    presetHint.textContent = "Manual tuning active. Current values will be saved as a custom profile.";
    return;
  }
  applyPresetToForm(selectedPreset);
  presetHint.textContent = `${selectedPreset} preset applied to the form. Save changes to persist it.`;
});

settingsForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.selectedGuildId) {
    return;
  }
  try {
    const detail = normalizeGuildDetail(await api(`${API_BASE}/guilds/${state.selectedGuildId}`, {
      method: "PUT",
      body: JSON.stringify(buildPayload()),
    }));
    state.detail = detail;
    syncGuildSummary(detail);
    renderGuildList();
    renderDetail(detail);
    showToast("Settings saved.", "success");
  } catch (error) {
    if (/Unauthorized|HTTP 401/i.test(String(error.message || ""))) {
      await loadSession();
    }
    showToast(error.message || "Failed to save settings.", "error");
  }
});

async function api(path, options = {}) {
  const headers = {
    ...(options.headers || {}),
  };
  if (options.body && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }
  let response;
  try {
    response = await fetch(path, {
      credentials: "same-origin",
      ...options,
      headers,
    });
  } catch (error) {
    throw new Error("Network request failed.");
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (payload.errors) {
      const message = Object.entries(payload.errors)
        .map(([field, issue]) => `${field}: ${issue}`)
        .join(" | ");
      throw new Error(message);
    }
    throw new Error(payload.error || `Request failed with HTTP ${response.status}.`);
  }
  return payload;
}

async function loadSession() {
  try {
    const payload = await api(`${API_BASE}/session`);
    state.authenticated = Boolean(payload.authenticated);
    state.mode = payload.mode || null;
    state.user = payload.user || null;
    state.continental = payload.continental || null;
    state.license = payload.license || null;
    state.continentalLoginUrl = payload.continental_login_url || "";
    state.continentalDashboardUrl = payload.continental_dashboard_url || "";
    state.continentalAuthEnabled = Boolean(payload.continental_auth_enabled);
    renderAuthStatus(payload);
    renderIntegrationSummary();
    return payload;
  } catch (error) {
    state.authenticated = false;
    state.mode = null;
    state.user = null;
    state.continental = null;
    state.license = null;
    state.continentalLoginUrl = "";
    state.continentalDashboardUrl = "";
    state.continentalAuthEnabled = false;
    renderAuthStatus({ authenticated: false, continental_auth_enabled: false });
    renderIntegrationSummary();
    return null;
  }
}

async function loadGuilds() {
  if (!state.authenticated) {
    showToast("Sign in with Continental ID first.", "error");
    return;
  }
  try {
    const payload = await api(`${API_BASE}/guilds`);
    state.guilds = Array.isArray(payload.guilds)
      ? payload.guilds.map(normalizeGuildSummary).filter(Boolean)
      : [];
    state.license = payload.license || state.license;
    renderIntegrationSummary();
    renderGuildList();
    const hashGuildId = window.location.hash.replace("#guild-", "");
    const currentGuildId = state.guilds.find(
      (guild) => String(guild.id) === String(state.selectedGuildId)
    )?.id;
    const preferredGuildId =
      state.guilds.find((guild) => String(guild.id) === hashGuildId)?.id ||
      currentGuildId ||
      state.guilds[0]?.id ||
      null;
    if (preferredGuildId) {
      await selectGuild(preferredGuildId);
    } else {
      state.selectedGuildId = null;
      state.detail = null;
      window.location.hash = "";
      dashboard.classList.add("hidden");
      emptyState.classList.remove("hidden");
      renderDashboardNotice(null);
    }
  } catch (error) {
    state.guilds = [];
    state.selectedGuildId = null;
    state.detail = null;
    renderGuildList();
    dashboard.classList.add("hidden");
    emptyState.classList.remove("hidden");
    renderDashboardNotice(null);
    await loadSession();
    showToast(error.message || "Failed to load guilds.", "error");
  }
}

function renderGuildList() {
  guildList.innerHTML = "";
  if (!state.guilds.length) {
    guildList.innerHTML = `<p class="field-help">No guilds available for this bot session.</p>`;
    return;
  }
  for (const guild of state.guilds) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "guild-card";
    if (String(guild.id) === String(state.selectedGuildId)) {
      button.classList.add("active");
    }
    button.innerHTML = `
      <strong>${escapeHtml(guild.name)}</strong>
      <span>${guild.member_count} members</span>
      <span>Guard ${guild.guard_enabled ? "enabled" : "disabled"} • preset ${escapeHtml(guild.guard_preset)}</span>
      <span class="${guild.authorization && !guild.authorization.authorized ? "guild-card-alert" : ""}">${
        guild.authorization && !guild.authorization.authorized
          ? escapeHtml(guild.authorization.reason || "Restricted by current access policy.")
          : "Guild is currently within Vanguard access scope."
      }</span>
    `;
    button.addEventListener("click", () => selectGuild(guild.id));
    guildList.appendChild(button);
  }
}

async function selectGuild(guildId) {
  const normalizedGuildId = String(guildId);
  const previousSelectedGuildId = state.selectedGuildId;
  const previousDetail = state.detail;
  const requestToken = ++state.guildRequestToken;
  state.selectedGuildId = normalizedGuildId;
  renderGuildList();
  try {
    const detail = normalizeGuildDetail(await api(`${API_BASE}/guilds/${normalizedGuildId}`));
    if (requestToken !== state.guildRequestToken) {
      return;
    }
    state.detail = detail;
    window.location.hash = `guild-${detail.id}`;
    renderDetail(detail);
  } catch (error) {
    if (requestToken !== state.guildRequestToken) {
      return;
    }
    const recoveryApplied = shouldRecoverMissingGuild(error)
      ? recoverFromMissingGuild(normalizedGuildId, previousDetail)
      : false;
    if (!recoveryApplied) {
      state.selectedGuildId = previousSelectedGuildId || null;
      renderGuildList();
    }
    if (/Unauthorized|HTTP 401/i.test(String(error.message || ""))) {
      await loadSession();
    }
    showToast(error.message || "Failed to load guild detail.", "error");
  }
}

function shouldRecoverMissingGuild(error) {
  return /Guild not found|Forbidden|HTTP 403|HTTP 404/i.test(String(error?.message || ""));
}

function renderDetail(detail) {
  emptyState.classList.add("hidden");
  dashboard.classList.remove("hidden");

  document.querySelector("#guild-name").textContent = detail.name;
  document.querySelector("#guild-meta").textContent =
    `${detail.member_count} members • ${detail.recent_cases_24h} cases in the last 24h`;
  document.querySelector("#guard-preset-badge").textContent = detail.settings.guard_preset;
  document.querySelector("#last-trigger-badge").textContent = formatTimestamp(
    detail.runtime_stats.last_trigger_at
  );
  document.querySelector("#stat-members").textContent = detail.member_count;
  document.querySelector("#stat-votes").textContent = detail.active_votes;
  document.querySelector("#stat-reminders").textContent = detail.pending_reminders;
  document.querySelector("#stat-cases").textContent = detail.recent_cases_24h;
  document.querySelector("#stat-triggers").textContent = detail.runtime_stats.triggers_total;
  document.querySelector("#stat-suppressed").textContent = detail.runtime_stats.suppressed_total;
  if (guildAccessBadge) {
    guildAccessBadge.textContent =
      detail.authorization && !detail.authorization.authorized ? "Restricted" : "Authorized";
  }
  if (detail.license) {
    state.license = detail.license;
    renderIntegrationSummary();
  }
  renderDashboardNotice(detail);
  renderLockdownTarget(detail);

  populateSelect("welcome-channel", detail.channels, "Channel not set");
  populateSelect("ops-channel", detail.channels, "Channel not set");
  populateSelect("log-channel", detail.channels, "Channel not set");
  populateSelect("welcome-role", detail.roles, "Role not set");
  populateSelect("lockdown-role", detail.roles, "Use @everyone");
  populateModRoles(detail.roles, detail.settings.mod_role_ids || []);
  fillForm(detail);
}

async function runLockdownAction(locked) {
  if (!state.selectedGuildId) {
    return;
  }

  setLockdownActionState(true);
  try {
    const payload = await api(`${API_BASE}/guilds/${state.selectedGuildId}/lockdown`, {
      method: "POST",
      body: JSON.stringify({ locked }),
    });
    const detail = normalizeGuildDetail(payload.detail);
    state.detail = detail;
    syncGuildSummary(detail);
    renderGuildList();
    renderDetail(detail);
    showToast(
      payload.message || (locked ? "Server lockdown triggered." : "Server lockdown lifted."),
      "success"
    );
  } catch (error) {
    if (/Unauthorized|HTTP 401/i.test(String(error.message || ""))) {
      await loadSession();
    }
    showToast(error.message || "Failed to update server lockdown.", "error");
  } finally {
    setLockdownActionState(false);
  }
}

function fillForm(detail) {
  const settings = detail.settings;
  for (const [key, elementId] of Object.entries(fieldIds)) {
    const element = document.querySelector(`#${elementId}`);
    if (!element) {
      continue;
    }
    if (element.tagName === "TEXTAREA") {
      element.value = settings[key] || "";
    } else {
      element.value = settings[key] || "";
    }
  }

  const guard = settings.guard;
  for (const [key, elementId] of guardNumberFields) {
    document.querySelector(`#${elementId}`).value = guard[key];
  }
  for (const [key, elementId] of guardCheckboxFields) {
    document.querySelector(`#${elementId}`).checked = Boolean(guard[key]);
  }
  document.querySelector("#guard-slowmode-scope").value = guard.guard_slowmode_scope;
  guardPresetSelect.value = settings.guard_preset || "custom";
  presetHint.textContent =
    settings.guard_preset === "custom"
      ? "Manual tuning active. Current values will be saved as a custom profile."
      : `${settings.guard_preset} preset is active.`;
}

function populateSelect(elementId, items, emptyLabel) {
  const select = document.querySelector(`#${elementId}`);
  select.innerHTML = "";
  const emptyOption = document.createElement("option");
  emptyOption.value = "";
  emptyOption.textContent = emptyLabel;
  select.appendChild(emptyOption);
  for (const item of items) {
    const option = document.createElement("option");
    option.value = item.id;
    option.textContent = `#${item.name}`.replace("##", "#");
    if (item.mention.startsWith("<@&")) {
      option.textContent = item.name;
    }
    select.appendChild(option);
  }
}

function populateModRoles(roles, selectedRoleIds) {
  const container = document.querySelector("#mod-role-grid");
  container.innerHTML = "";
  const selected = new Set((selectedRoleIds || []).map((roleId) => String(roleId)));
  for (const role of roles) {
    const label = document.createElement("label");
    label.className = "role-option";
    label.innerHTML = `
      <input type="checkbox" value="${role.id}" />
      <span>${escapeHtml(role.name)}</span>
    `;
    const input = label.querySelector("input");
    input.checked = selected.has(String(role.id));
    container.appendChild(label);
  }
}

function renderLockdownTarget(detail) {
  if (!lockdownTargetDetail) {
    return;
  }
  const lockdownRoleId = detail.settings.lockdown_role_id;
  const targetRole = detail.roles.find((role) => String(role.id) === String(lockdownRoleId));
  const targetLabel = targetRole ? `@${targetRole.name}` : "@everyone";
  lockdownTargetDetail.textContent =
    `Trigger a live server lockdown for ${targetLabel}. This updates channel send permissions immediately.`;
}

function buildPayload() {
  const payload = {
    welcome_channel_id: readOptionalSelect("welcome-channel"),
    welcome_role_id: readOptionalSelect("welcome-role"),
    welcome_message: document.querySelector("#welcome-message").value,
    ops_channel_id: readOptionalSelect("ops-channel"),
    log_channel_id: readOptionalSelect("log-channel"),
    lockdown_role_id: readOptionalSelect("lockdown-role"),
    mod_role_ids: [...document.querySelectorAll("#mod-role-grid input:checked")].map((input) => input.value),
    guard_preset: guardPresetSelect.value,
    guard: {
      guard_slowmode_scope: document.querySelector("#guard-slowmode-scope").value,
    },
  };

  for (const [key, elementId] of guardNumberFields) {
    payload.guard[key] = Number(document.querySelector(`#${elementId}`).value);
  }
  for (const [key, elementId] of guardCheckboxFields) {
    payload.guard[key] = document.querySelector(`#${elementId}`).checked;
  }
  return payload;
}

function applyPresetToForm(presetName) {
  const preset = PRESETS[presetName];
  if (!preset) {
    return;
  }
  for (const [key, value] of Object.entries(preset)) {
    const numberField = guardNumberFields.find(([fieldKey]) => fieldKey === key);
    const checkboxField = guardCheckboxFields.find(([fieldKey]) => fieldKey === key);
    if (numberField) {
      document.querySelector(`#${numberField[1]}`).value = value;
    } else if (checkboxField) {
      document.querySelector(`#${checkboxField[1]}`).checked = Boolean(value);
    } else if (key === "guard_slowmode_scope") {
      document.querySelector("#guard-slowmode-scope").value = value;
    }
  }
}

function readOptionalSelect(elementId) {
  const raw = document.querySelector(`#${elementId}`).value;
  return raw || null;
}

function syncGuildSummary(detail) {
  const index = state.guilds.findIndex((guild) => String(guild.id) === String(detail.id));
  if (index === -1) {
    return;
  }
  state.guilds[index] = {
    ...state.guilds[index],
    guard_enabled: detail.guard_enabled,
    guard_preset: detail.settings.guard_preset,
    recent_cases_24h: detail.recent_cases_24h,
    pending_reminders: detail.pending_reminders,
    active_votes: detail.active_votes,
    runtime_stats: detail.runtime_stats,
    authorization: detail.authorization || state.guilds[index].authorization,
  };
}

function recoverFromMissingGuild(guildId, previousDetail) {
  const beforeCount = state.guilds.length;
  state.guilds = state.guilds.filter((guild) => String(guild.id) !== String(guildId));
  if (state.guilds.length === beforeCount) {
    return false;
  }

  const previousGuildStillAvailable =
    previousDetail &&
    state.guilds.find((guild) => String(guild.id) === String(previousDetail.id));
  if (previousGuildStillAvailable) {
    state.selectedGuildId = String(previousDetail.id);
    state.detail = previousDetail;
    renderGuildList();
    renderDetail(previousDetail);
  } else if (state.guilds.length) {
    state.selectedGuildId = String(state.guilds[0].id);
    state.detail = null;
    renderGuildList();
    window.setTimeout(() => {
      selectGuild(state.selectedGuildId);
    }, 0);
  } else {
    state.selectedGuildId = null;
    state.detail = null;
    renderGuildList();
    window.location.hash = "";
    dashboard.classList.add("hidden");
    emptyState.classList.remove("hidden");
    renderDashboardNotice(null);
  }
  return true;
}

function toFiniteNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeSnowflake(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  return String(value);
}

function normalizeSnowflakeList(values) {
  if (!Array.isArray(values)) {
    return [];
  }
  const seen = new Set();
  const normalized = [];
  for (const value of values) {
    const snowflake = normalizeSnowflake(value);
    if (!snowflake || seen.has(snowflake)) {
      continue;
    }
    seen.add(snowflake);
    normalized.push(snowflake);
  }
  return normalized;
}

function normalizeRuntimeStats(runtimeStats) {
  const stats = runtimeStats && typeof runtimeStats === "object" ? runtimeStats : {};
  return {
    triggers_total: toFiniteNumber(stats.triggers_total, 0),
    suppressed_total: toFiniteNumber(stats.suppressed_total, 0),
    last_trigger_at: stats.last_trigger_at || null,
    last_trigger_reasons: Array.isArray(stats.last_trigger_reasons) ? stats.last_trigger_reasons : [],
    last_trigger_severity: String(stats.last_trigger_severity || "none"),
    last_trigger_actor_id: normalizeSnowflake(stats.last_trigger_actor_id),
  };
}

function normalizeAuthorization(authorization) {
  const normalized = authorization && typeof authorization === "object" ? authorization : {};
  return {
    authorized: normalized.authorized !== false,
    source: String(normalized.source || "default"),
    reason: String(normalized.reason || ""),
  };
}

function normalizeGuardSettings(guard) {
  const normalized = {
    ...UI_GUARD_DEFAULTS,
  };
  const source = guard && typeof guard === "object" ? guard : {};
  for (const [key] of guardNumberFields) {
    normalized[key] = toFiniteNumber(source[key], normalized[key]);
  }
  for (const [key] of guardCheckboxFields) {
    normalized[key] = Boolean(source[key]);
  }
  normalized.guard_slowmode_scope =
    source.guard_slowmode_scope === "active" ? "active" : UI_GUARD_DEFAULTS.guard_slowmode_scope;
  return normalized;
}

function normalizeGuildSummary(guild) {
  if (!guild || typeof guild !== "object") {
    return null;
  }
  const guildId = normalizeSnowflake(guild.id);
  if (!guildId) {
    return null;
  }
  return {
    id: guildId,
    name: String(guild.name || "Server"),
    icon_url: guild.icon_url || null,
    member_count: toFiniteNumber(guild.member_count, 0),
    guard_enabled: Boolean(guild.guard_enabled),
    guard_preset: String(guild.guard_preset || "custom"),
    active_votes: toFiniteNumber(guild.active_votes, 0),
    pending_reminders: toFiniteNumber(guild.pending_reminders, 0),
    recent_cases_24h: toFiniteNumber(guild.recent_cases_24h, 0),
    runtime_stats: normalizeRuntimeStats(guild.runtime_stats),
    authorization: normalizeAuthorization(guild.authorization),
  };
}

function normalizeGuildDetail(detail) {
  const summary = normalizeGuildSummary(detail);
  if (!summary) {
    throw new Error("Invalid guild detail payload.");
  }
  const settings = detail.settings && typeof detail.settings === "object" ? detail.settings : {};
  return {
    ...summary,
    license: detail.license || null,
    channels: Array.isArray(detail.channels)
      ? detail.channels
          .map((channel) => {
            const channelId = normalizeSnowflake(channel && channel.id);
            if (!channelId) {
              return null;
            }
            return {
              id: channelId,
              name: String(channel.name || "unknown"),
              mention: String(channel.mention || `<#${channelId}>`),
              position: toFiniteNumber(channel.position, 0),
            };
          })
          .filter(Boolean)
      : [],
    roles: Array.isArray(detail.roles)
      ? detail.roles
          .map((role) => {
            const roleId = normalizeSnowflake(role && role.id);
            if (!roleId) {
              return null;
            }
            return {
              id: roleId,
              name: String(role.name || "unknown"),
              mention: String(role.mention || `<@&${roleId}>`),
              position: toFiniteNumber(role.position, 0),
              color: toFiniteNumber(role.color, 0),
            };
          })
          .filter(Boolean)
      : [],
    settings: {
      welcome_channel_id: normalizeSnowflake(settings.welcome_channel_id),
      welcome_role_id: normalizeSnowflake(settings.welcome_role_id),
      welcome_message: String(settings.welcome_message || ""),
      ops_channel_id: normalizeSnowflake(settings.ops_channel_id),
      log_channel_id: normalizeSnowflake(settings.log_channel_id),
      lockdown_role_id: normalizeSnowflake(settings.lockdown_role_id),
      mod_role_ids: normalizeSnowflakeList(settings.mod_role_ids),
      guard_preset: String(settings.guard_preset || "custom"),
      guard: normalizeGuardSettings(settings.guard),
    },
  };
}

function renderAuthStatus(session) {
  if (state.authenticated && state.user) {
    authStatus.textContent = `Signed in as ${state.user.name}. Only guilds your linked Discord account can moderate are shown.`;
    logoutButton.classList.remove("hidden");
    continentalLogin.classList.add("hidden");
    return;
  }
  if (session && session.continental_auth_enabled === false) {
    authStatus.textContent = "Continental ID sign-in is not configured on this instance.";
  } else {
    authStatus.textContent = "Sign in with Continental ID. Discord must be linked on that account.";
  }
  logoutButton.classList.add("hidden");
  continentalLogin.classList.toggle("hidden", session && session.continental_auth_enabled === false);
}

function describeContinentalStatus(continental) {
  if (!continental || !continental.configured) {
    return {
      badge: "Disabled",
      detail: "Continental ID integration is not configured for this Vanguard instance.",
    };
  }
  if (!state.authenticated) {
    return {
      badge: "Awaiting sign-in",
      detail: "Sign in with Continental ID, then make sure Discord is linked on that account.",
    };
  }
  if (!continental.ok) {
    return {
      badge: "Unavailable",
      detail: continental.message || "Continental ID lookup is currently unavailable.",
    };
  }
  if (!continental.linked) {
    const dashboardText = state.continentalDashboardUrl
      ? ` Link Discord in Continental ID: ${state.continentalDashboardUrl}`
      : "";
    return {
      badge: "Not linked",
      detail:
        (continental.message || "Your Continental ID account is not linked to Discord.") + dashboardText,
    };
  }

  const flags = continental.flags || {};
  let badge = "Linked";
  if (flags.banned_from_ai) {
    badge = "Restricted";
  } else if (flags.flagged) {
    badge = "Review";
  } else if (flags.staff) {
    badge = "Staff";
  } else if (flags.trusted) {
    badge = "Trusted";
  }

  const identityBits = [];
  if (continental.user && continental.user.display_name) {
    identityBits.push(continental.user.display_name);
  }
  if (continental.user && continental.user.username) {
    identityBits.push(`@${continental.user.username}`);
  }
  if (continental.user && continental.user.verified) {
    identityBits.push("verified");
  }

  let detail = identityBits.length
    ? `Linked as ${identityBits.join(" • ")}.`
    : "Linked to Continental ID.";
  if (flags.banned_from_ai) {
    detail += " This account is restricted from Vanguard AI features.";
  } else if (flags.flagged) {
    detail += flags.flag_reason
      ? ` Brand standing is under review: ${flags.flag_reason}.`
      : " Brand standing is currently under review.";
  } else if (flags.staff) {
    detail += " Staff standing is active.";
  } else if (flags.trusted) {
    detail += " Trusted standing is active.";
  }

  return { badge, detail };
}

function describeLicenseState(license) {
  if (!license) {
    return {
      badge: "Disabled",
      detail: "Vanguard license status is not available.",
    };
  }

  if (license.required && !license.authorized) {
    return {
      badge: "Blocked",
      detail: license.reason || "This Vanguard instance is blocked by its required license check.",
    };
  }

  if (license.allowed_guild_count && !license.configured && !license.required) {
    return {
      badge: "Allowlist",
      detail: `${license.allowed_guild_count} guild(s) are explicitly authorized on this instance.`,
    };
  }

  if (!license.configured && !license.required) {
    return {
      badge: "Disabled",
      detail: "No remote license verification is configured for this Vanguard instance.",
    };
  }

  const badge = license.required ? "Active" : "Monitor";
  const detailBits = [];
  if (license.reason) {
    detailBits.push(license.reason);
  }
  if (license.allowed_guild_count) {
    detailBits.push(`${license.allowed_guild_count} guild(s) allowed`);
  }
  const entitlements = [];
  if (license.entitlements && license.entitlements.ai) {
    entitlements.push("AI");
  }
  if (license.entitlements && license.entitlements.advanced_votes) {
    entitlements.push("advanced votes");
  }
  if (license.entitlements && Array.isArray(license.entitlements.guard_presets) && license.entitlements.guard_presets.length) {
    entitlements.push(`guard presets: ${license.entitlements.guard_presets.join(", ")}`);
  }
  if (entitlements.length) {
    detailBits.push(`Entitlements: ${entitlements.join(" • ")}`);
  }

  return {
    badge,
    detail: detailBits.join(" | ") || "License state is configured.",
  };
}

function renderIntegrationSummary() {
  const continental = describeContinentalStatus(state.continental);
  if (continentalBadge) {
    continentalBadge.textContent = continental.badge;
  }
  if (continentalDetail) {
    continentalDetail.textContent = continental.detail;
  }

  const license = describeLicenseState(state.license);
  if (licenseBadge) {
    licenseBadge.textContent = license.badge;
  }
  if (licenseDetail) {
    licenseDetail.textContent = license.detail;
  }
}

function setLockdownActionState(isBusy) {
  if (lockdownEnableButton) {
    lockdownEnableButton.disabled = isBusy;
  }
  if (lockdownDisableButton) {
    lockdownDisableButton.disabled = isBusy;
  }
}

function renderDashboardNotice(detail) {
  if (!dashboardNotice) {
    return;
  }

  const authorization = detail && detail.authorization ? detail.authorization : null;
  if (!authorization || authorization.authorized) {
    dashboardNotice.classList.add("hidden");
    dashboardNotice.textContent = "";
    return;
  }

  dashboardNotice.textContent = authorization.reason || "This guild is currently outside the active Vanguard access scope.";
  dashboardNotice.classList.remove("hidden");
}

function buildContinentalPopupUrl(baseUrl) {
  const url = new URL(baseUrl, window.location.origin);
  url.searchParams.set("origin", window.location.origin);
  return url.toString();
}

function getContinentalLoginOrigin() {
  if (!state.continentalLoginUrl) {
    return "";
  }
  try {
    return new URL(state.continentalLoginUrl, window.location.origin).origin;
  } catch (error) {
    return "";
  }
}

async function exchangeContinentalSession(accessToken) {
  const payload = await api(`${API_BASE}/session/exchange`, {
    method: "POST",
    body: JSON.stringify({ accessToken }),
  });
  state.authenticated = Boolean(payload.authenticated);
  state.mode = payload.mode || null;
  state.user = payload.user || null;
  state.continental = payload.continental || null;
  state.license = payload.license || null;
  renderAuthStatus({
    authenticated: state.authenticated,
    continental_auth_enabled: state.continentalAuthEnabled,
  });
  renderIntegrationSummary();
  await loadGuilds();
}

window.addEventListener("message", async (event) => {
  const expectedOrigin = getContinentalLoginOrigin();
  if (!expectedOrigin || event.origin !== expectedOrigin) {
    return;
  }
  const payload = event.data || {};
  if (payload.type !== "LOGIN_SUCCESS") {
    return;
  }
  const accessToken = String(payload.accessToken || payload.token || "").trim();
  if (!accessToken) {
    showToast("Continental ID did not return a usable access token.", "error");
    return;
  }
  try {
    await exchangeContinentalSession(accessToken);
    showToast("Signed in through Continental ID.", "success");
  } catch (error) {
    const message = error.message || "Continental ID sign-in could not be completed.";
    showToast(message, "error");
    if (
      state.continentalDashboardUrl &&
      /discord linked|link discord|linked before it can access/i.test(message)
    ) {
      window.open(state.continentalDashboardUrl, "_blank", "noopener,noreferrer");
    }
  }
});

function formatTimestamp(value) {
  if (!value) {
    return "Never";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "Unknown";
  }
  return parsed.toLocaleString();
}

function showToast(message, tone) {
  toast.textContent = message;
  toast.className = `toast ${tone || ""}`;
  toast.classList.remove("hidden");
  window.clearTimeout(showToast.timeoutId);
  showToast.timeoutId = window.setTimeout(() => {
    toast.classList.add("hidden");
  }, 3600);
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
