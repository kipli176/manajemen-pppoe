(function () {
  function $(id) {
    return document.getElementById(id);
  }

  var rootEl = $("billingRoot");
  var routerId = Number((rootEl && rootEl.getAttribute("data-router-id")) || 0);
  var routerLabel = String((rootEl && rootEl.getAttribute("data-router-label")) || "Router");
  var billingLocked = String((rootEl && rootEl.getAttribute("data-billing-locked")) || "0") === "1";
  var billingWarning = String((rootEl && rootEl.getAttribute("data-billing-warning")) || "");

  var state = {
    tab: "active",
    active: [],
    secrets: [],
    profiles: [],
    payments: [],
    paymentSettings: {
      auto_close_unpaid_end_month: false
    },
    logs: [],
    editing: null,
    loadingCount: 0,
    paySelection: null,
    deletingSecretName: null,
    cache: {
      active: { loaded: false, at: 0, promise: null },
      profiles: { loaded: false, at: 0, promise: null },
      secrets: { loaded: false, at: 0, promise: null },
      payments: { loaded: false, at: 0, promise: null },
      logs: { loaded: false, at: 0, promise: null }
    }
  };
  var CACHE_TTL_MS = {
    active: 12000,
    profiles: 180000,
    secrets: 90000,
    payments: 90000,
    logs: 90000
  };

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function setStatus(msg, isError) {
    var el = $("billingStatus");
    if (!el) return;
    el.textContent = msg || "";
    el.style.color = isError ? "#b91c1c" : "#334155";
  }

  function formatRupiah(value) {
    var amount = Number(value || 0);
    if (!Number.isFinite(amount)) amount = 0;
    return "Rp " + new Intl.NumberFormat("id-ID", { maximumFractionDigits: 0 }).format(amount);
  }

  function parseRupiahInput(raw) {
    var cleaned = String(raw || "").replace(/[^0-9]/g, "");
    return cleaned ? Number(cleaned) : 0;
  }

  function parseUptimeSeconds(raw) {
    var text = String(raw || "").trim();
    if (!text) return null;

    var colonMatch = text.match(/^(?:(\d+)w)?(?:(\d+)d)?(\d{1,2}):(\d{2}):(\d{2})$/i);
    if (colonMatch) {
      var weeks = Number(colonMatch[1] || 0);
      var days = Number(colonMatch[2] || 0);
      var hours = Number(colonMatch[3] || 0);
      var minutes = Number(colonMatch[4] || 0);
      var seconds = Number(colonMatch[5] || 0);
      return (weeks * 7 * 24 * 3600) + (days * 24 * 3600) + (hours * 3600) + (minutes * 60) + seconds;
    }

    var totalSeconds = 0;
    var found = false;
    var unitMap = { w: 7 * 24 * 3600, d: 24 * 3600, h: 3600, m: 60, s: 1 };
    var tokenRegex = /(\d+)([wdhms])/gi;
    var token;
    while ((token = tokenRegex.exec(text)) !== null) {
      var value = Number(token[1] || 0);
      var factor = unitMap[(token[2] || "").toLowerCase()];
      if (value && factor) {
        totalSeconds += value * factor;
        found = true;
      }
    }
    return found ? totalSeconds : null;
  }

  function formatUptime(raw) {
    var total = parseUptimeSeconds(raw);
    if (total === null) return "-";
    if (total <= 0) return "0s";

    var weeks = Math.floor(total / (7 * 24 * 3600));
    total = total % (7 * 24 * 3600);
    var days = Math.floor(total / (24 * 3600));
    total = total % (24 * 3600);
    var hours = Math.floor(total / 3600);
    total = total % 3600;
    var minutes = Math.floor(total / 60);
    var seconds = total % 60;

    var parts = [];
    if (weeks) parts.push(weeks + "w");
    if (days) parts.push(days + "d");
    if (hours) {
      parts.push(hours + "j");
    } else if ((weeks || days) && (minutes || seconds)) {
      // Supaya konsisten: jika sudah ada hari/minggu dan ada menit/detik, tampilkan jam meski 0.
      parts.push("0j");
    }
    if (minutes) parts.push(minutes + "m");
    if (seconds && parts.length < 2) parts.push(seconds + "s");
    if (!parts.length) parts.push("0s");
    return parts.slice(0, 3).join(" ");
  }

  function formatActiveSince(rawUptime) {
    var total = parseUptimeSeconds(rawUptime);
    if (total === null) return "-";
    var startedAt = new Date(Date.now() - (total * 1000));
    if (Number.isNaN(startedAt.getTime())) return "-";
    try {
      return new Intl.DateTimeFormat("id-ID", {
        day: "2-digit",
        month: "short",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
      }).format(startedAt);
    } catch (_err) {
      return startedAt.toISOString().slice(0, 16).replace("T", " ");
    }
  }

  function formatLastLogout(raw) {
    var text = String(raw || "").trim();
    if (!text) return "-";
    var match = text.match(/^([a-z]{3})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}:\d{2}:\d{2}))?$/i);
    if (!match) return text;
    var monthMap = {
      jan: "Jan", feb: "Feb", mar: "Mar", apr: "Apr",
      may: "Mei", jun: "Jun", jul: "Jul", aug: "Agu",
      sep: "Sep", oct: "Okt", nov: "Nov", dec: "Des"
    };
    var month = monthMap[(match[1] || "").toLowerCase()] || match[1];
    var day = String(match[2]).padStart(2, "0");
    var year = match[3];
    var time = match[4] || "";
    return day + " " + month + " " + year + (time ? " " + time : "");
  }

  function formatAuditTime(raw) {
    var text = String(raw || "").trim();
    if (!text) return "-";
    return text.replace("T", " ").slice(0, 19);
  }

  function setLoading(visible, text) {
    var wrap = $("billingLoading");
    var label = $("billingLoadingText");
    if (!wrap) return;
    if (label && text) label.textContent = text;
    if (visible) {
      wrap.classList.remove("hidden");
      wrap.classList.add("flex");
    } else {
      wrap.classList.add("hidden");
      wrap.classList.remove("flex");
    }
  }

  function beginLoading(text) {
    state.loadingCount += 1;
    setLoading(true, text || "Memuat data...");
  }

  function endLoading() {
    state.loadingCount = Math.max(0, Number(state.loadingCount || 0) - 1);
    if (state.loadingCount === 0) {
      setLoading(false);
    }
  }

  function apiFetch(path, options, meta) {
    var opts = options || {};
    var cfg = meta || {};
    var showLoader = cfg.showLoader !== false;
    if (showLoader) {
      beginLoading(cfg.loadingText || "Memuat data...");
    }
    var req = Object.assign({ headers: { "Content-Type": "application/json" } }, opts);
    return fetch(path, req).then(function (res) {
      return res.json().then(function (data) {
        if (!res.ok || !data.ok) {
          throw new Error((data && data.message) || ("HTTP " + res.status));
        }
        return data;
      });
    }).finally(function () {
      if (showLoader) {
        endLoading();
      }
    });
  }

  function isFreshCache(key, force) {
    var cacheItem = state.cache[key];
    if (!cacheItem) return false;
    if (force) return false;
    if (!cacheItem.loaded) return false;
    var ttl = Number(CACHE_TTL_MS[key] || 0);
    if (ttl <= 0) return false;
    return (Date.now() - Number(cacheItem.at || 0)) < ttl;
  }

  function markCacheLoaded(key) {
    var cacheItem = state.cache[key];
    if (!cacheItem) return;
    cacheItem.loaded = true;
    cacheItem.at = Date.now();
  }

  function withCacheInFlight(key, fetcher) {
    var cacheItem = state.cache[key];
    if (!cacheItem) return fetcher();
    if (cacheItem.promise) return cacheItem.promise;
    cacheItem.promise = Promise.resolve()
      .then(fetcher)
      .finally(function () {
        cacheItem.promise = null;
      });
    return cacheItem.promise;
  }

  function setTab(tab, options) {
    var opts = options || {};
    if (billingLocked && tab !== "active" && tab !== "guide") {
      setStatus(billingWarning, true);
      tab = "active";
    }
    state.tab = tab;
    var tabMap = {
      active: "tabActive",
      secret: "tabSecret",
      profile: "tabProfile",
      payment: "tabPayment",
      log: "tabLog",
      guide: "tabGuide"
    };
    Object.keys(tabMap).forEach(function (key) {
      var btn = $(tabMap[key]);
      if (!btn) return;
      if (key === tab) {
        btn.className = "rounded-lg bg-primary px-3 py-2 text-xs font-semibold text-white";
      } else {
        btn.className = "rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-semibold text-slate-700";
      }
    });
    $("panelActive").classList.toggle("hidden", tab !== "active");
    $("panelSecret").classList.toggle("hidden", tab !== "secret");
    $("panelProfile").classList.toggle("hidden", tab !== "profile");
    $("panelPayment").classList.toggle("hidden", tab !== "payment");
    $("panelLog").classList.toggle("hidden", tab !== "log");
    $("panelGuide").classList.toggle("hidden", tab !== "guide");

    if (!opts.skipRefresh) {
      refreshTabData(tab, true, false);
    }
  }

  function refreshTabData(tab, silent, force) {
    if (tab === "active") return loadActive(silent, force);
    if (tab === "secret") return loadSecrets(silent, force);
    if (tab === "profile") return loadProfiles(silent, force);
    if (tab === "payment") return loadPayments(silent, force);
    if (tab === "log") return loadLogs(silent, force);
    if (tab === "guide") return Promise.resolve();
    return Promise.resolve();
  }

  function renderKpi() {
    $("kpiActive").textContent = String(state.active.length);
    var secretCount = state.secrets.length;
    if (!secretCount && state.payments.length) {
      secretCount = state.payments.length;
    }
    $("kpiSecret").textContent = String(secretCount);
    var totalReceived = state.payments.reduce(function (acc, item) {
      return acc + Number(item.total_paid || 0);
    }, 0);
    $("kpiRevenue").textContent = formatRupiah(totalReceived);
    var due = state.payments.reduce(function (acc, item) { return acc + Number(item.total_due || 0); }, 0);
    $("kpiDue").textContent = formatRupiah(due);
  }

  function renderProfiles() {
    var body = $("profileBody");
    if (!body) return;
    if (!state.profiles.length) {
      body.innerHTML = "<tr><td colspan='4' class='px-3 py-6 text-center text-slate-500'>Tidak ada profile.</td></tr>";
    } else {
      body.innerHTML = state.profiles.map(function (item, i) {
        var rowClass = "table-row-ui";
        return "<tr class='" + rowClass + "'>" +
          "<td class='px-3 py-2'>" + (i + 1) + "</td>" +
          "<td class='px-3 py-2'>" + esc(item.name || "-") + "</td>" +
          "<td class='px-3 py-2'>" + esc(item["rate-limit"] || "-") + "</td>" +
          "<td class='px-3 py-2'>" + esc(item["only-one"] || "-") + "</td>" +
          "</tr>";
      }).join("");
    }

    var select = $("secretFormProfileInput");
    if (!select) return;
    if (!state.profiles.length) {
      select.innerHTML = "<option value='default'>default</option>";
    } else {
      select.innerHTML = state.profiles.map(function (item) {
        var n = String(item.name || "");
        return "<option value='" + esc(n) + "'>" + esc(n) + "</option>";
      }).join("");
    }
  }

  function renderActive() {
    var body = $("activeBody");
    var badge = $("activeCountBadge");
    if (badge) {
      badge.textContent = String(state.active.length) + " user";
    }
    if (!body) return;
    if (!state.active.length) {
      body.innerHTML = "<tr><td colspan='7' class='px-3 py-6 text-center text-slate-500'>Tidak ada user active.</td></tr>";
      return;
    }
    body.innerHTML = state.active.map(function (item, i) {
      var rowClass = "table-row-ui";
      var activeId = item[".id"] || item.id || "";
      var btn = billingLocked
        ? "<span class='text-xs text-slate-500'>Read-only</span>"
        : "<button class='rounded-lg border border-rose-200 bg-rose-50 px-2 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-100' data-action='disconnect-active' data-id='" + esc(activeId) + "' data-name='" + esc(item.name || "") + "'>&#128268; Disconnect</button>";
      return "<tr class='" + rowClass + "'>" +
        "<td class='px-3 py-2'>" + (i + 1) + "</td>" +
        "<td class='px-3 py-2'>" + esc(item.name || "-") + "</td>" +
        "<td class='px-3 py-2'>" + esc(item.address || "-") + "</td>" +
        "<td class='px-3 py-2'>" + esc(formatUptime(item.uptime)) + "</td>" +
        "<td class='px-3 py-2'>" + esc(formatActiveSince(item.uptime)) + "</td>" +
        "<td class='px-3 py-2'>" + esc(item.service || "-") + "</td>" +
        "<td class='px-3 py-2'><div class='table-actions'>" + btn + "</div></td>" +
        "</tr>";
    }).join("");
  }

  function setEditMode(editing) {
    state.editing = editing || null;
  }

  function isUserOnline(username) {
    var target = String(username || "").trim();
    if (!target) return false;
    return state.active.some(function (entry) {
      return String((entry && entry.name) || "").trim() === target;
    });
  }

  function onlineBadgeHtml(isOnline) {
    if (isOnline) {
      return "<span class='ml-1 inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-1.5 py-0.5 text-[10px] font-semibold text-emerald-700' title='Sedang online'>\uD83D\uDFE2 online</span>";
    }
    return "<span class='ml-1 inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-[10px] font-semibold text-slate-500' title='Sedang offline'>\u26AA offline</span>";
  }

  function openSecretModal() {
    var modal = $("secretModal");
    if (!modal) return;
    modal.classList.remove("hidden");
    modal.classList.add("flex");
  }

  function closeSecretModal() {
    var modal = $("secretModal");
    if (!modal) return;
    modal.classList.add("hidden");
    modal.classList.remove("flex");
    setEditMode(null);
  }

  function openSecretAddModal(profilesReady) {
    var ready = profilesReady === true;
    if (!ready && !state.profiles.length) {
      loadProfiles(true, false).finally(function () {
        openSecretAddModal(true);
      });
      return;
    }
    setEditMode(null);
    var titleEl = $("secretModalTitle");
    var hintEl = $("secretModalHint");
    var submitBtn = $("secretModalSubmitBtn");
    if (titleEl) titleEl.textContent = "\u270D\uFE0F Tambah PPP Secret";
    if (hintEl) hintEl.textContent = "Password wajib diisi untuk user baru.";
    if (submitBtn) submitBtn.textContent = "\uD83D\uDCBE Simpan";

    var nameInput = $("secretFormNameInput");
    if (nameInput) {
      nameInput.value = "";
      nameInput.readOnly = false;
      nameInput.classList.remove("bg-slate-100", "text-slate-500", "cursor-not-allowed");
    }
    $("secretFormPasswordInput").value = "";
    $("secretFormLocalAddressInput").value = "";
    $("secretFormRemoteAddressInput").value = "";
    var profileSelect = $("secretFormProfileInput");
    if (profileSelect) {
      if (profileSelect.options.length <= 0) {
        profileSelect.innerHTML = "<option value='default'>default</option>";
      }
      profileSelect.selectedIndex = 0;
    }
    openSecretModal();
  }

  function openSecretEditModal(name, profilesReady) {
    if (!profilesReady && !state.profiles.length) {
      loadProfiles(true, false).finally(function () {
        openSecretEditModal(name, true);
      });
      return;
    }
    var item = state.secrets.find(function (entry) {
      return String(entry.name || "") === String(name || "");
    });
    if (!item) {
      setStatus("Data secret tidak ditemukan.", true);
      return;
    }
    setEditMode({ currentName: String(item.name || "") });

    var titleEl = $("secretModalTitle");
    var hintEl = $("secretModalHint");
    var submitBtn = $("secretModalSubmitBtn");
    if (titleEl) titleEl.textContent = "\u270F\uFE0F Edit PPP Secret";
    if (hintEl) hintEl.textContent = "Username hanya-baca. Password boleh kosong jika tidak diubah.";
    if (submitBtn) submitBtn.textContent = "\uD83D\uDCBE Simpan Perubahan";

    var nameInput = $("secretFormNameInput");
    if (nameInput) {
      nameInput.value = String(item.name || "");
      nameInput.readOnly = true;
      nameInput.classList.add("bg-slate-100", "text-slate-500", "cursor-not-allowed");
    }
    $("secretFormPasswordInput").value = "";
    var profileSelect = $("secretFormProfileInput");
    if (profileSelect) {
      var currentProfile = String(item.profile || "");
      var hasProfileOption = Array.prototype.some.call(profileSelect.options || [], function (opt) {
        return String(opt.value || "") === currentProfile;
      });
      if (currentProfile && !hasProfileOption) {
        var option = document.createElement("option");
        option.value = currentProfile;
        option.textContent = currentProfile;
        profileSelect.appendChild(option);
      }
      profileSelect.value = currentProfile;
    }
    $("secretFormLocalAddressInput").value = String(item["local-address"] || "");
    $("secretFormRemoteAddressInput").value = String(item["remote-address"] || "");
    openSecretModal();
  }

  function openSecretDeleteModal(name) {
    var modal = $("secretDeleteModal");
    var nameEl = $("secretDeleteName");
    if (!modal) return;
    state.deletingSecretName = String(name || "");
    if (nameEl) nameEl.textContent = state.deletingSecretName || "-";
    modal.classList.remove("hidden");
    modal.classList.add("flex");
  }

  function closeSecretDeleteModal() {
    var modal = $("secretDeleteModal");
    if (!modal) return;
    modal.classList.add("hidden");
    modal.classList.remove("flex");
    state.deletingSecretName = null;
  }

  function confirmDeleteSecret() {
    var removeName = String(state.deletingSecretName || "").trim();
    if (!removeName) {
      closeSecretDeleteModal();
      return;
    }
    closeSecretDeleteModal();
    apiFetch("/billing/api/" + routerId + "/secrets/remove", {
      method: "POST",
      body: JSON.stringify({ name: removeName })
    })
      .then(function () { return Promise.all([loadSecrets(true, true), loadActive(true, true), loadPayments(true, true), loadLogs(true, true)]); })
      .then(function () { setStatus("Secret berhasil dihapus.", false); })
      .catch(function (err) { setStatus(err.message, true); });
  }

  function renderSecrets() {
    var body = $("secretBody");
    if (!body) return;
    if (!state.secrets.length) {
      body.innerHTML = "<tr><td colspan='8' class='px-3 py-6 text-center text-slate-500'>Belum ada user secret.</td></tr>";
      return;
    }
    body.innerHTML = state.secrets.map(function (item, i) {
      var rowClass = "table-row-ui";
      var name = String(item.name || "");
      var profile = String(item.profile || "");
      var localAddress = String(item["local-address"] || "-");
      var remoteAddress = String(item["remote-address"] || "-");
      var isOnline = isUserOnline(name);
      var disabled = String(item.disabled || "no").toLowerCase();
      var isDisabled = disabled === "yes" || disabled === "true";
      return "<tr class='" + rowClass + "'>" +
        "<td class='px-3 py-2'>" + (i + 1) + "</td>" +
        "<td class='px-3 py-2'>" + esc(name || "-") + onlineBadgeHtml(isOnline) + "</td>" +
        "<td class='px-3 py-2'>" + esc(profile || "-") + "</td>" +
        "<td class='px-3 py-2'>" + esc(localAddress) + "</td>" +
        "<td class='px-3 py-2'>" + esc(remoteAddress) + "</td>" +
        "<td class='px-3 py-2'>" + (isDisabled ? "yes" : "no") + "</td>" +
        "<td class='px-3 py-2'>" + esc(formatLastLogout(item["last-logged-out"])) + "</td>" +
        "<td class='px-3 py-2'>" +
          "<button class='rounded-lg border border-amber-200 bg-amber-50 px-2 py-1 text-xs font-semibold text-amber-700 hover:bg-amber-100' data-action='edit-secret' data-name='" + esc(name) + "'>&#9998;&#65039; Edit</button> " +
          "<button class='rounded-lg border border-slate-200 bg-slate-50 px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-100' data-action='toggle-secret' data-name='" + esc(name) + "' data-disabled='" + (isDisabled ? "1" : "0") + "'>" + (isDisabled ? "&#9989; Enable" : "&#128683; Disable") + "</button> " +
          "<button class='rounded-lg border border-rose-200 bg-rose-50 px-2 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-100' data-action='remove-secret' data-name='" + esc(name) + "'>&#128465;&#65039; Remove</button>" +
        "</td>" +
      "</tr>";
    }).join("");
  }

  function renderPayments() {
    renderPaymentPreferences();
    var body = $("paymentBody");
    if (!body) return;
    if (!state.payments.length) {
      body.innerHTML = "<tr><td colspan='7' class='px-3 py-6 text-center text-slate-500'>Belum ada data pembayaran.</td></tr>";
      return;
    }
    body.innerHTML = state.payments.map(function (item, i) {
      var rowClass = "table-row-ui";
      var unpaid = Number(item.unpaid_count || 0);
      var isOnline = Boolean(item.is_online) || isUserOnline(item.name || "");
      var hasPaid = Boolean(item.paid_until_month);
      var actions = "";
      if (unpaid > 0) {
        actions += "<button class='rounded-lg border border-emerald-200 bg-emerald-50 px-2 py-1 text-xs font-semibold text-emerald-700 hover:bg-emerald-100' data-action='pay-user' data-name='" + esc(item.name || "") + "'>&#128176; Bayar</button> ";
      }
      if (hasPaid) {
        actions += "<button class='rounded-lg border border-rose-200 bg-rose-50 px-2 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-100' data-action='cancel-pay-user' data-name='" + esc(item.name || "") + "'>&#8617;&#65039; Batal Bayar</button>";
      }

      return "<tr class='" + rowClass + "'>" +
        "<td class='px-3 py-2'>" + (i + 1) + "</td>" +
        "<td class='px-3 py-2'>" + esc(item.name || "-") + onlineBadgeHtml(isOnline) + "</td>" +
        "<td class='px-3 py-2'>" + esc(item.profile || "-") + "</td>" +
        "<td class='px-3 py-2'><input class='rounded-lg border border-slate-300 px-2 py-1 text-xs' data-action='fee-input' data-name='" + esc(item.name || "") + "' data-current='" + Number(item.monthly_fee || 0) + "' value='" + esc(formatRupiah(item.monthly_fee || 0)) + "'></td>" +
        "<td class='px-3 py-2'>" + esc(item.status || "-") + "</td>" +
        "<td class='px-3 py-2'>" + esc(formatRupiah(item.total_due || 0)) + "</td>" +
        "<td class='px-3 py-2'><div class='table-actions'>" + actions + "</div></td>" +
      "</tr>";
    }).join("");
  }

  function renderPaymentPreferences() {
    var toggle = $("paymentAutoCloseToggle");
    var info = $("paymentAutoCloseInfo");
    if (!toggle) return;
    var enabled = Boolean(state.paymentSettings && state.paymentSettings.auto_close_unpaid_end_month);
    toggle.checked = enabled;
    if (billingLocked) {
      toggle.disabled = true;
    }
    if (info) {
      info.textContent = enabled ? "Aktif: akhir bulan 18:00 akan auto-lunas" : "Nonaktif";
      info.className = enabled
        ? "text-[11px] font-semibold text-emerald-700"
        : "text-[11px] text-slate-500";
    }
  }

  function printPaymentsSheet() {
    var printNow = function () {
      if (!state.payments.length) {
        setStatus("Data pembayaran kosong, tidak ada yang bisa dicetak.", true);
        return;
      }
      var nowLabel;
      try {
        nowLabel = new Intl.DateTimeFormat("id-ID", {
          day: "2-digit",
          month: "long",
          year: "numeric"
        }).format(new Date());
      } catch (_err) {
        nowLabel = new Date().toISOString().slice(0, 10);
      }
      var totalTagihan = state.payments.length;
      var totalSudahBayar = state.payments.filter(function (item) {
        return Number(item.unpaid_count || 0) === 0;
      }).length;
      var totalBelumBayar = Math.max(0, totalTagihan - totalSudahBayar);
      var totalTagihanNominal = state.payments.reduce(function (acc, item) {
        return acc + Number(item.monthly_fee || 0);
      }, 0);
      var totalSudahBayarNominal = state.payments.reduce(function (acc, item) {
        if (Number(item.unpaid_count || 0) === 0) {
          return acc + Number(item.monthly_fee || 0);
        }
        return acc;
      }, 0);
      var totalBelumBayarNominal = state.payments.reduce(function (acc, item) {
        return acc + Number(item.total_due || 0);
      }, 0);

      var rowsHtml = state.payments.map(function (item, idx) {
        var statusText = String(item.status || "-");
        var isPaid = Number(item.unpaid_count || 0) === 0 || /lunas|sudah bayar/i.test(statusText);
        var checkMark = isPaid ? "&#9745;" : "&#9633;";
        return "<tr>" +
          "<td>" + (idx + 1) + "</td>" +
          "<td>" + esc(item.name || "-") + "</td>" +
          "<td>" + esc(formatRupiah(item.monthly_fee || 0)) + "</td>" +
          "<td>" + esc(statusText) + "</td>" +
          "<td class='nominal-cell'></td>" +
          "<td class='check-cell'>" + checkMark + "</td>" +
          "</tr>";
      }).join("");

      var printWin = window.open("", "_blank", "width=1024,height=720");
      if (!printWin) {
        setStatus("Popup diblokir browser. Izinkan popup untuk fitur print.", true);
        return;
      }

      printWin.document.open();
      printWin.document.write(
        "<!doctype html><html><head><meta charset='utf-8'>" +
        "<title>Print Pembayaran - " + esc(routerLabel) + "</title>" +
        "<style>" +
        "@page { size: A4 portrait; margin: 12mm; }" +
        "body { font-family: Arial, sans-serif; color: #0f172a; }" +
        "h1 { margin: 0 0 6px; font-size: 18px; }" +
        "p { margin: 0; font-size: 12px; color: #334155; }" +
        ".meta { margin: 0 0 10px; display: flex; justify-content: space-between; align-items: center; gap: 12px; }" +
        ".meta-right { text-align: right; font-size: 12px; color: #334155; }" +
        ".meta-right .highlight { font-weight: 800; color: #0f172a; }" +
        "table { width: 100%; border-collapse: collapse; font-size: 12px; }" +
        "th, td { border: 1px solid #94a3b8; padding: 7px 6px; text-align: left; vertical-align: top; }" +
        "th { background: #f1f5f9; font-weight: 700; }" +
        ".center { text-align: center; }" +
        ".nominal-cell { min-width: 120px; height: 28px; }" +
        ".check-cell { text-align: center; font-size: 18px; width: 72px; }" +
        "</style></head><body>" +
        "<h1>Daftar Pembayaran - " + esc(routerLabel) + "</h1>" +
        "<div class='meta'>" +
        "<p>Tanggal cetak: " + esc(nowLabel) + "</p>" +
        "<div class='meta-right'>" +
        "Jumlah tagihan: <b>" + totalTagihan + "</b> (" + esc(formatRupiah(totalTagihanNominal)) + ") | " +
        "Sudah bayar: <b>" + totalSudahBayar + "</b> (" + esc(formatRupiah(totalSudahBayarNominal)) + ") | " +
        "Belum bayar: <span class='highlight'>" + totalBelumBayar + "</span> (" + esc(formatRupiah(totalBelumBayarNominal)) + ")" +
        "</div>" +
        "</div>" +
        "<table><thead><tr>" +
        "<th class='center' style='width:40px;'>No</th>" +
        "<th>Nama</th>" +
        "<th style='width:130px;'>Bulanan</th>" +
        "<th>Status Pembayaran</th>" +
        "<th style='width:130px;'>Nominal</th>" +
        "<th class='center' style='width:80px;'>Ceklist</th>" +
        "</tr></thead><tbody>" +
        rowsHtml +
        "</tbody></table>" +
        "</body></html>"
      );
      printWin.document.close();
      printWin.focus();
      setTimeout(function () {
        printWin.print();
        printWin.close();
      }, 300);
    };

    if (!state.payments.length) {
      loadPayments(false, true).then(printNow);
      return;
    }
    printNow();
  }

  function openPayMonthModal(name) {
    var item = state.payments.find(function (entry) {
      return String(entry.name || "") === String(name || "");
    });
    if (!item) {
      setStatus("Data pembayaran user tidak ditemukan.", true);
      return;
    }

    var values = Array.isArray(item.unpaid_month_values) ? item.unpaid_month_values : [];
    var labels = Array.isArray(item.unpaid_months) ? item.unpaid_months : [];
    if (!values.length) {
      paymentAction(name, "pay", null, null, true);
      return;
    }

    var modal = $("payMonthModal");
    var userEl = $("payMonthUser");
    var selectEl = $("payMonthSelect");
    var hintEl = $("payMonthHint");
    if (!modal || !userEl || !selectEl) return;

    userEl.textContent = String(name || "-");
    selectEl.innerHTML = values.map(function (monthValue, idx) {
      var monthLabel = labels[idx] || monthValue;
      return "<option value='" + esc(monthValue) + "'>" + esc(monthLabel) + "</option>";
    }).join("");
    if (hintEl) {
      hintEl.textContent = "Pilih bulan terakhir yang ingin dianggap lunas. Sistem akan membayar berurutan sampai bulan itu.";
    }

    state.paySelection = { name: String(name || "") };
    modal.classList.remove("hidden");
    modal.classList.add("flex");
  }

  function closePayMonthModal() {
    var modal = $("payMonthModal");
    if (!modal) return;
    modal.classList.add("hidden");
    modal.classList.remove("flex");
    state.paySelection = null;
  }

  function submitPayMonthModal() {
    if (!state.paySelection || !state.paySelection.name) {
      closePayMonthModal();
      return;
    }
    var selectEl = $("payMonthSelect");
    if (!selectEl) {
      closePayMonthModal();
      return;
    }
    var targetMonth = String(selectEl.value || "").trim();
    var label = selectEl.options[selectEl.selectedIndex]
      ? selectEl.options[selectEl.selectedIndex].text
      : targetMonth;
    var selectedName = state.paySelection.name;
    closePayMonthModal();
    paymentAction(selectedName, "pay", targetMonth, label, true);
  }

  function renderLogs() {
    var body = $("logBody");
    if (!body) return;
    if (!state.logs.length) {
      body.innerHTML = "<tr><td colspan='5' class='px-3 py-6 text-center text-slate-500'>Belum ada log audit.</td></tr>";
      return;
    }
    body.innerHTML = state.logs.map(function (item, i) {
      var rowClass = "table-row-ui";
      return "<tr class='" + rowClass + "'>" +
        "<td class='px-3 py-2'>" + (i + 1) + "</td>" +
        "<td class='px-3 py-2'>" + esc(formatAuditTime(item.created_at)) + "</td>" +
        "<td class='px-3 py-2'>" + esc(item.secret_name || "-") + "</td>" +
        "<td class='px-3 py-2'>" + esc(item.action || "-") + "</td>" +
        "<td class='px-3 py-2'>" + esc(item.detail || "-") + "</td>" +
      "</tr>";
    }).join("");
  }

  function loadActive(silent, force) {
    var quiet = Boolean(silent);
    var forceRefresh = Boolean(force);
    if (isFreshCache("active", forceRefresh)) {
      renderActive();
      renderKpi();
      return Promise.resolve();
    }
    if (!quiet) setStatus("Memuat active connection...", false);
    return withCacheInFlight("active", function () {
      return apiFetch(
        "/billing/api/" + routerId + "/active",
        null,
        { showLoader: !quiet, loadingText: "Memuat active connection..." }
      ).then(function (data) {
        state.active = data.items || [];
        markCacheLoaded("active");
        renderActive();
        renderKpi();
        if (!quiet) setStatus("Active loaded: " + state.active.length + " user", false);
      });
    }).catch(function (err) {
      setStatus(err.message, true);
    });
  }

  function loadProfiles(silent, force) {
    if (billingLocked) return Promise.resolve();
    var quiet = Boolean(silent);
    var forceRefresh = Boolean(force);
    if (isFreshCache("profiles", forceRefresh)) {
      renderProfiles();
      return Promise.resolve();
    }
    if (!quiet) setStatus("Memuat profile...", false);
    return withCacheInFlight("profiles", function () {
      return apiFetch(
        "/billing/api/" + routerId + "/profiles",
        null,
        { showLoader: !quiet, loadingText: "Memuat profile..." }
      ).then(function (data) {
        state.profiles = data.items || [];
        markCacheLoaded("profiles");
        renderProfiles();
        renderKpi();
        if (!quiet) setStatus("Profile loaded: " + state.profiles.length, false);
      });
    }).catch(function (err) {
      setStatus(err.message, true);
    });
  }

  function loadSecrets(silent, force) {
    if (billingLocked) return Promise.resolve();
    var quiet = Boolean(silent);
    var forceRefresh = Boolean(force);
    if (isFreshCache("secrets", forceRefresh)) {
      renderSecrets();
      renderKpi();
      return Promise.resolve();
    }
    if (!quiet) setStatus("Memuat PPP secret...", false);
    return Promise.all([
      loadActive(true, false),
      withCacheInFlight("secrets", function () {
        return apiFetch(
          "/billing/api/" + routerId + "/secrets",
          null,
          { showLoader: !quiet, loadingText: "Memuat PPP secret..." }
        );
      })
    ])
      .then(function (results) {
        var data = results[1] || {};
        state.secrets = data.items || [];
        markCacheLoaded("secrets");
        renderSecrets();
        renderKpi();
        if (!quiet) setStatus("Secret loaded: " + state.secrets.length + " user", false);
      })
      .catch(function (err) {
        setStatus(err.message, true);
      });
  }

  function loadPayments(silent, force) {
    if (billingLocked) return Promise.resolve();
    var quiet = Boolean(silent);
    var forceRefresh = Boolean(force);
    if (isFreshCache("payments", forceRefresh)) {
      renderPayments();
      renderKpi();
      return Promise.resolve();
    }
    if (!quiet) setStatus("Memuat pembayaran...", false);
    return Promise.all([
      loadActive(true, false),
      withCacheInFlight("payments", function () {
        return apiFetch(
          "/billing/api/" + routerId + "/payments",
          null,
          { showLoader: !quiet, loadingText: "Memuat pembayaran..." }
        );
      })
    ])
      .then(function (results) {
        var data = results[1] || {};
        state.payments = data.items || [];
        state.paymentSettings = data.settings || state.paymentSettings || { auto_close_unpaid_end_month: false };
        markCacheLoaded("payments");
        renderPayments();
        renderKpi();
        if (!quiet) setStatus("Pembayaran loaded: " + state.payments.length + " user", false);
      })
      .catch(function (err) {
        setStatus(err.message, true);
      });
  }

  function loadLogs(silent, force) {
    if (billingLocked) return Promise.resolve();
    var quiet = Boolean(silent);
    var forceRefresh = Boolean(force);
    if (isFreshCache("logs", forceRefresh)) {
      renderLogs();
      return Promise.resolve();
    }
    if (!quiet) setStatus("Memuat log audit...", false);
    return withCacheInFlight("logs", function () {
      return apiFetch(
        "/billing/api/" + routerId + "/logs",
        null,
        { showLoader: !quiet, loadingText: "Memuat log audit..." }
      ).then(function (data) {
        state.logs = data.items || [];
        markCacheLoaded("logs");
        renderLogs();
        if (!quiet) setStatus("Log loaded: " + state.logs.length + " baris", false);
      });
    }).catch(function (err) {
      setStatus(err.message, true);
    });
  }

  function submitSecret() {
    if (billingLocked) return;
    var nameInput = $("secretFormNameInput");
    var pwInput = $("secretFormPasswordInput");
    var profileInput = $("secretFormProfileInput");
    var localAddressInput = $("secretFormLocalAddressInput");
    var remoteAddressInput = $("secretFormRemoteAddressInput");
    var rawName = (nameInput.value || "").trim();
    var name = state.editing ? String(state.editing.currentName || rawName) : rawName;
    var password = pwInput.value || "";
    var profile = (profileInput.value || "").trim();
    var localAddress = (localAddressInput.value || "").trim();
    var remoteAddress = (remoteAddressInput.value || "").trim();
    if (!name || !profile) {
      setStatus("Isi username dan profile.", true);
      return;
    }
    if (!state.editing && !password) {
      setStatus("Password wajib untuk tambah user.", true);
      return;
    }
    var path = "/billing/api/" + routerId + "/secrets/" + (state.editing ? "edit" : "add");
    var payload = state.editing
      ? {
        current_name: state.editing.currentName,
        new_name: String(state.editing.currentName || name),
        new_password: password.trim(),
        new_profile: profile,
        new_local_address: localAddress,
        new_remote_address: remoteAddress
      }
      : {
        name: name,
        password: password,
        profile: profile,
        local_address: localAddress,
        remote_address: remoteAddress
      };
    apiFetch(path, { method: "POST", body: JSON.stringify(payload) })
      .then(function () {
        closeSecretModal();
        return Promise.all([loadSecrets(true, true), loadActive(true, true), loadPayments(true, true), loadLogs(true, true)]);
      })
      .then(function () {
        setStatus("Data secret berhasil disimpan.", false);
      })
      .catch(function (err) {
        setStatus(err.message, true);
      });
  }

  function updateFee(name, nextValue, inputEl, currentValue) {
    beginLoading("Menyimpan paket bulanan...");
    apiFetch(
      "/billing/api/" + routerId + "/payments/fee",
      {
        method: "POST",
        body: JSON.stringify({ name: name, monthly_fee: nextValue })
      },
      { showLoader: false }
    )
      .then(function () {
        inputEl.setAttribute("data-current", String(nextValue));
        inputEl.value = formatRupiah(nextValue);
        return Promise.all([loadPayments(true, true), loadLogs(true, true)]);
      })
      .then(function () {
        setStatus("Paket bulanan diperbarui.", false);
      })
      .catch(function (err) {
        inputEl.value = formatRupiah(currentValue);
        setStatus(err.message, true);
      })
      .finally(function () {
        endLoading();
      });
  }

  function updatePaymentPreferences(nextEnabled, toggleEl) {
    var previous = Boolean(state.paymentSettings && state.paymentSettings.auto_close_unpaid_end_month);
    if (toggleEl) toggleEl.disabled = true;
    apiFetch(
      "/billing/api/" + routerId + "/payments/preferences",
      {
        method: "POST",
        body: JSON.stringify({ auto_close_unpaid_end_month: Boolean(nextEnabled) })
      }
    )
      .then(function (data) {
        state.paymentSettings = (data && data.settings) || { auto_close_unpaid_end_month: Boolean(nextEnabled) };
        renderPaymentPreferences();
        setStatus("Pengaturan auto-lunas akhir bulan diperbarui.", false);
      })
      .catch(function (err) {
        state.paymentSettings = { auto_close_unpaid_end_month: previous };
        renderPaymentPreferences();
        setStatus(err.message, true);
      })
      .finally(function () {
        if (toggleEl) toggleEl.disabled = false;
      });
  }

  function paymentAction(name, action, targetMonth, targetMonthLabel, skipConfirm) {
    var label = action === "pay" ? "Bayar" : "Batal bayar";
    var confirmText = label + " user " + name + "?";
    if (action === "pay" && targetMonthLabel) {
      confirmText = "Bayar user " + name + " sampai " + targetMonthLabel + "?";
    }
    if (!skipConfirm && !window.confirm(confirmText)) return;
    var payload = { name: name, action: action };
    if (targetMonth) payload.target_month = targetMonth;
    apiFetch("/billing/api/" + routerId + "/payments/action", {
      method: "POST",
      body: JSON.stringify(payload)
    })
      .then(function () { return Promise.all([loadPayments(true, true), loadLogs(true, true)]); })
      .then(function () { setStatus("Pembayaran diperbarui.", false); })
      .catch(function (err) { setStatus(err.message, true); });
  }

  function disconnectActive(id, name) {
    if (!window.confirm("Disconnect user " + name + "?")) return;
    apiFetch("/billing/api/" + routerId + "/active/disconnect", {
      method: "POST",
      body: JSON.stringify({ id: id || null, name: name || null })
    })
      .then(function (data) {
        return loadActive(true, true).then(function () { return data; });
      })
      .then(function (data) {
        var total = Number((data && data.removed) || 0);
        if (total > 0) {
          setStatus("User active berhasil di-disconnect.", false);
        } else {
          setStatus("User tidak ditemukan di active list saat proses disconnect.", true);
        }
      })
      .catch(function (err) { setStatus(err.message, true); });
  }

  function requestQrisToAdmin() {
    var btn = $("requestQrisBtn");
    var info = $("requestQrisInfo");
    if (!btn) return;
    btn.disabled = true;
    btn.textContent = "Mengirim...";
    if (info) info.textContent = "";

    apiFetch(
      "/billing/api/" + routerId + "/request-qris",
      { method: "POST" },
      { showLoader: true, loadingText: "Mengirim permintaan QRIS ke admin..." }
    )
      .then(function (data) {
        setStatus("Permintaan QRIS berhasil dikirim ke admin.", false);
        if (info) {
          info.textContent = "Permintaan QRIS sudah terkirim. Tunggu balasan admin.";
        }
        var sentAdmin = Number(data.sent_admin || 0);
        if (sentAdmin <= 0 && info) {
          info.textContent = "Permintaan terkirim, tapi admin belum menerima. Coba ulang.";
        }
      })
      .catch(function (err) {
        setStatus(err.message, true);
        if (info) info.textContent = "Gagal kirim permintaan QRIS.";
      })
      .finally(function () {
        btn.disabled = false;
        btn.textContent = "\uD83D\uDCE8 Kirim Permintaan QRIS ke Admin";
      });
  }

  function initEvents() {
    $("tabActive").addEventListener("click", function () { setTab("active"); });
    $("tabSecret").addEventListener("click", function () { setTab("secret"); });
    $("tabProfile").addEventListener("click", function () { setTab("profile"); });
    $("tabPayment").addEventListener("click", function () { setTab("payment"); });
    $("tabLog").addEventListener("click", function () { setTab("log"); });
    $("tabGuide").addEventListener("click", function () { setTab("guide"); });

    var qrisBtn = $("requestQrisBtn");
    if (qrisBtn) {
      qrisBtn.addEventListener("click", requestQrisToAdmin);
    }
    var paymentPrintBtn = $("paymentPrintBtn");
    if (paymentPrintBtn) {
      paymentPrintBtn.addEventListener("click", printPaymentsSheet);
    }
    var autoCloseToggle = $("paymentAutoCloseToggle");
    if (autoCloseToggle) {
      autoCloseToggle.addEventListener("change", function () {
        updatePaymentPreferences(Boolean(autoCloseToggle.checked), autoCloseToggle);
      });
    }
    var payCancelBtn = $("payMonthCancelBtn");
    if (payCancelBtn) {
      payCancelBtn.addEventListener("click", closePayMonthModal);
    }
    var paySubmitBtn = $("payMonthSubmitBtn");
    if (paySubmitBtn) {
      paySubmitBtn.addEventListener("click", submitPayMonthModal);
    }
    var payModal = $("payMonthModal");
    if (payModal) {
      payModal.addEventListener("click", function (ev) {
        if (ev.target === payModal) {
          closePayMonthModal();
        }
      });
    }

    var secretOpenAddBtn = $("secretOpenAddBtn");
    if (secretOpenAddBtn) {
      secretOpenAddBtn.addEventListener("click", function () {
        openSecretAddModal(false);
      });
    }
    var secretModalCancelBtn = $("secretModalCancelBtn");
    if (secretModalCancelBtn) {
      secretModalCancelBtn.addEventListener("click", closeSecretModal);
    }
    var secretModalSubmitBtn = $("secretModalSubmitBtn");
    if (secretModalSubmitBtn) {
      secretModalSubmitBtn.addEventListener("click", submitSecret);
    }
    var secretDeleteCancelBtn = $("secretDeleteCancelBtn");
    if (secretDeleteCancelBtn) {
      secretDeleteCancelBtn.addEventListener("click", closeSecretDeleteModal);
    }
    var secretDeleteConfirmBtn = $("secretDeleteConfirmBtn");
    if (secretDeleteConfirmBtn) {
      secretDeleteConfirmBtn.addEventListener("click", confirmDeleteSecret);
    }
    var secretModal = $("secretModal");
    if (secretModal) {
      secretModal.addEventListener("click", function (ev) {
        if (ev.target === secretModal) {
          closeSecretModal();
        }
      });
    }
    var secretDeleteModal = $("secretDeleteModal");
    if (secretDeleteModal) {
      secretDeleteModal.addEventListener("click", function (ev) {
        if (ev.target === secretDeleteModal) {
          closeSecretDeleteModal();
        }
      });
    }

    document.addEventListener("click", function (ev) {
      var t = ev.target;
      if (!(t instanceof Element)) return;

      if (t.matches('[data-action="disconnect-active"]')) {
        disconnectActive(t.getAttribute("data-id") || "", t.getAttribute("data-name") || "");
        return;
      }
      if (t.matches('[data-action="edit-secret"]')) {
        openSecretEditModal(t.getAttribute("data-name") || "");
        return;
      }
      if (t.matches('[data-action="toggle-secret"]')) {
        var name = t.getAttribute("data-name") || "";
        var disabledNow = t.getAttribute("data-disabled") === "1";
        apiFetch("/billing/api/" + routerId + "/secrets/disable", {
          method: "POST",
          body: JSON.stringify({ name: name, disabled: !disabledNow })
        })
          .then(function () { return Promise.all([loadSecrets(true, true), loadActive(true, true), loadPayments(true, true)]); })
          .then(function () { setStatus("Status secret diperbarui.", false); })
          .catch(function (err) { setStatus(err.message, true); });
        return;
      }
      if (t.matches('[data-action="remove-secret"]')) {
        openSecretDeleteModal(t.getAttribute("data-name") || "");
        return;
      }
      if (t.matches('[data-action="pay-user"]')) {
        openPayMonthModal(t.getAttribute("data-name") || "");
        return;
      }
      if (t.matches('[data-action="cancel-pay-user"]')) {
        paymentAction(t.getAttribute("data-name") || "", "cancel_pay", null, null, false);
      }
    });

    document.addEventListener("focusin", function (ev) {
      var t = ev.target;
      if (!(t instanceof HTMLInputElement)) return;
      if (!t.matches('[data-action="fee-input"]')) return;
      t.value = String(Number(t.getAttribute("data-current") || 0));
    });

    document.addEventListener("keydown", function (ev) {
      var t = ev.target;
      if (!(t instanceof HTMLInputElement)) return;
      if (!t.matches('[data-action="fee-input"]')) return;
      if (ev.key !== "Enter") return;
      ev.preventDefault();
      t.blur();
    });

    document.addEventListener("focusout", function (ev) {
      var t = ev.target;
      if (!(t instanceof HTMLInputElement)) return;
      if (!t.matches('[data-action="fee-input"]')) return;
      var name = t.getAttribute("data-name") || "";
      var currentValue = Number(t.getAttribute("data-current") || 0);
      var nextValue = parseRupiahInput(t.value);
      if (nextValue === currentValue) {
        t.value = formatRupiah(currentValue);
        return;
      }
      updateFee(name, nextValue, t, currentValue);
    });
  }

  function init() {
    beginLoading("Menyiapkan dashboard billing...");
    initEvents();
    if (billingLocked) {
      $("tabSecret").setAttribute("disabled", "disabled");
      $("tabProfile").setAttribute("disabled", "disabled");
      $("tabPayment").setAttribute("disabled", "disabled");
      $("tabLog").setAttribute("disabled", "disabled");
      setStatus(billingWarning + " (Auto refresh 1 menit)", true);
    }
    setTab("active", { skipRefresh: true });
    setEditMode(null);

    loadActive(false)
      .then(function () {
        if (!billingLocked) {
          // Optimasi awal: cukup preload pembayaran untuk KPI, tab lain lazy-load saat dibuka.
          return loadPayments(true, false);
        }
        return Promise.resolve();
      })
      .finally(function () {
        endLoading();
      });

    setInterval(function () {
      if (state.tab === "active") loadActive(true, true);
    }, 10000);

    if (billingLocked) {
      setInterval(function () {
        window.location.reload();
      }, 60000);
    }
  }

  init();
})();
