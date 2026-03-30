(function () {
  var loadingCount = 0;

  function $(id) {
    return document.getElementById(id);
  }

  function show(el) {
    if (el) el.classList.remove("hidden");
  }

  function hide(el) {
    if (el) el.classList.add("hidden");
  }

  function toast(msg) {
    var t = $("toast");
    if (!t) return;
    t.textContent = msg;
    t.style.opacity = 1;
    setTimeout(function () { t.style.opacity = 0; }, 1800);
  }

  function formatRupiah(value) {
    var amount = Number(value || 0);
    if (!Number.isFinite(amount)) amount = 0;
    try {
      return "Rp " + new Intl.NumberFormat("id-ID", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
      }).format(amount);
    } catch (_err) {
      return "Rp " + String(Math.round(amount));
    }
  }

  function setAdminLoading(visible, text) {
    var el = $("adminLoading");
    var txt = $("adminLoadingText");
    if (!el) return;
    if (txt && text) txt.textContent = text;
    if (visible) {
      el.classList.remove("hidden");
      el.classList.add("flex");
    } else {
      el.classList.add("hidden");
      el.classList.remove("flex");
    }
  }

  function beginLoading(text) {
    loadingCount += 1;
    setAdminLoading(true, text || "Memproses...");
  }

  function endLoading() {
    loadingCount = Math.max(0, loadingCount - 1);
    if (loadingCount === 0) {
      setAdminLoading(false);
    }
  }

  function initDarkMode() {
    var toggle = $("darkModeToggle");
    if (!toggle) return;
    var moon = "\uD83C\uDF19";
    var sun = "\u2600\uFE0F";

    function setIcon(isDark) {
      toggle.textContent = isDark ? sun : moon;
      toggle.setAttribute("aria-label", isDark ? "Mode terang" : "Mode gelap");
    }

    var saved = localStorage.getItem("admin_dark_mode");
    if (saved === "1") {
      document.documentElement.classList.add("dark");
      setIcon(true);
    } else {
      setIcon(false);
    }

    toggle.addEventListener("click", function () {
      document.documentElement.classList.toggle("dark");
      var dark = document.documentElement.classList.contains("dark");
      localStorage.setItem("admin_dark_mode", dark ? "1" : "0");
      setIcon(dark);
      toast(dark ? "Mode gelap aktif" : "Mode terang aktif");
    });
  }

  function netStatus(msg, color) {
    var net = $("netStatus");
    if (!net) return;
    net.textContent = msg;
    net.style.background = color;
    net.style.opacity = 1;
    if (navigator.onLine) {
      setTimeout(function () { net.style.opacity = 0; }, 1800);
    }
  }

  function initNetStatus() {
    window.addEventListener("offline", function () { netStatus("\u26A0\uFE0F Anda offline", "#dc2626"); });
    window.addEventListener("online", function () { netStatus("\u2705 Koneksi aktif", "#16a34a"); });
    if (!navigator.onLine) {
      netStatus("\u26A0\uFE0F Anda offline", "#dc2626");
    }
  }

  function openRouterModal(payload) {
    var modal = $("routerModal");
    var form = $("routerForm");
    if (!modal || !form) return;

    var title = $("routerModalTitle");
    var submit = $("routerSubmitBtn");
    var idInput = $("routerIdInput");
    var labelInput = $("routerLabelInput");
    var addressInput = $("routerAddressInput");
    var ipInput = $("routerIpInput");
    var portInput = $("routerPortInput");
    var usernameInput = $("routerUsernameInput");
    var waInput = $("routerWaInput");
    var passwordInput = $("routerPasswordInput");

    if (title) title.textContent = "Edit Router MikroTik \u270F\uFE0F";
    if (submit) submit.textContent = "Simpan Perubahan";
    form.setAttribute("action", "/routers/edit");
    if (idInput) idInput.value = payload.id || "";
    if (labelInput) labelInput.value = payload.label || "";
    if (addressInput) addressInput.value = payload.address || "";
    if (ipInput) ipInput.value = payload.ip || "";
    if (portInput) portInput.value = payload.port || "8728";
    if (usernameInput) usernameInput.value = payload.username || "";
    if (waInput) waInput.value = payload.wa || "";
    if (passwordInput) passwordInput.value = "";
    if (passwordInput) passwordInput.placeholder = "Kosongkan jika tidak diubah";

    show(modal);
    modal.classList.add("flex");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("overflow-hidden");
  }

  function closeRouterModal() {
    var modal = $("routerModal");
    if (!modal) return;
    hide(modal);
    modal.classList.remove("flex");
    modal.setAttribute("aria-hidden", "true");
    document.body.classList.remove("overflow-hidden");
  }

  function openDeleteModal(payload) {
    var modal = $("deleteModal");
    var idInput = $("deleteRouterIdInput");
    var label = $("deleteRouterLabel");
    if (!modal) return;

    if (idInput) idInput.value = payload.id || "";
    if (label) label.textContent = payload.label || "-";

    show(modal);
    modal.classList.add("flex");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("overflow-hidden");
  }

  function closeDeleteModal() {
    var modal = $("deleteModal");
    if (!modal) return;
    hide(modal);
    modal.classList.remove("flex");
    modal.setAttribute("aria-hidden", "true");
    document.body.classList.remove("overflow-hidden");
  }

  function openBillingDetail(payload) {
    var modal = $("billingDetailModal");
    if (!modal) return;

    var setText = function (id, val) {
      var el = $(id);
      if (el) el.textContent = val;
    };

    setText("billingDetailLabel", payload.label || "-");
    setText("billingDetailStatus", payload.status || "-");
    setText("billingDetailRequired", payload.required || "-");
    setText("billingDetailPaidUntil", payload.paidUntil || "-");
    setText("billingDetailUserCount", String(payload.userCount || 0));
    setText("billingDetailMonthlyFee", formatRupiah(payload.monthlyFee || 0));
    setText("billingDetailUnpaidMonths", payload.unpaidMonths || "-");
    setText("billingDetailTotalDue", formatRupiah(payload.totalDue || 0));

    show(modal);
    modal.classList.add("flex");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("overflow-hidden");
  }

  function closeBillingDetail() {
    var modal = $("billingDetailModal");
    if (!modal) return;
    hide(modal);
    modal.classList.remove("flex");
    modal.setAttribute("aria-hidden", "true");
    document.body.classList.remove("overflow-hidden");
  }

  function initRouterBillingHeartbeat() {
    var ONE_HOUR_MS = 60 * 60 * 1000;

    function pingBillingCheck() {
      fetch("/api/router-billing/check", { method: "POST" })
        .then(function (res) { return res.json(); })
        .then(function (data) {
          if (!data || data.ok !== true) return;
          if (data.ran_billing) {
            toast("Tagihan router bulan ini diperbarui otomatis.");
            setTimeout(function () { window.location.reload(); }, 800);
            return;
          }
          if (data.ran_reminder) {
            toast("Notifikasi H-3 berhasil diproses.");
          }
        })
        .catch(function () {
          // Silent: heartbeat tidak boleh ganggu UX admin.
        });
    }

    pingBillingCheck();
    setInterval(pingBillingCheck, ONE_HOUR_MS);
  }

  function sendBillingNotify(routerId) {
    if (!routerId) {
      toast("Router tidak valid.");
      return;
    }
    var btn = $("billingNotifyBtn");
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Mengirim...";
    }
    beginLoading("Mengirim notifikasi WA...");
    fetch("/api/router-billing/notify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ router_id: Number(routerId) })
    })
      .then(function (res) { return res.json(); })
      .then(function (data) {
        if (!data || data.ok !== true) {
          throw new Error((data && data.message) || "Gagal kirim notifikasi");
        }
        var sentRouter = Number(data.sent_router || 0);
        var sentAdmin = Number(data.sent_admin || 0);
        if (sentRouter > 0) {
          toast("Notif terkirim ke router " + sentRouter + " dan admin " + sentAdmin + ".");
        } else if (sentAdmin > 0) {
          toast("Notif terkirim ke admin saja. Cek nomor WA router.");
        } else {
          toast("Tidak ada notifikasi yang berhasil dikirim.");
        }
      })
      .catch(function (err) {
        toast(err.message || "Gagal kirim notifikasi");
      })
      .finally(function () {
        if (btn) {
          btn.disabled = false;
          btn.textContent = "Kirim Notif WA";
        }
        endLoading();
      });
  }

  function initSubmitPace() {
    document.addEventListener("submit", function (event) {
      var form = event.target;
      if (!(form instanceof HTMLFormElement)) return;
      var action = String(form.getAttribute("action") || "");
      var method = String(form.getAttribute("method") || "get").toLowerCase();
      if (method !== "post") return;
      if (action.indexOf("/routers/") !== 0) return;

      var text = "Memproses data router...";
      if (action.indexOf("/routers/pay") === 0) text = "Memproses pembayaran router...";
      if (action.indexOf("/routers/delete") === 0) text = "Menghapus router...";
      if (action.indexOf("/routers/edit") === 0) text = "Menyimpan perubahan router...";
      beginLoading(text);
    });
  }

  document.addEventListener("click", function (event) {
    var target = event.target;
    if (!(target instanceof Element)) return;

    if (target.matches('[data-action="open-router-modal"]')) {
      openRouterModal({
        id: target.getAttribute("data-id") || "",
        label: target.getAttribute("data-label") || "",
        address: target.getAttribute("data-address") || "",
        ip: target.getAttribute("data-ip") || "",
        port: target.getAttribute("data-port") || "8728",
        username: target.getAttribute("data-username") || "",
        wa: target.getAttribute("data-wa") || ""
      });
      return;
    }

    if (target.matches('[data-action="close-router-modal"]')) {
      closeRouterModal();
      return;
    }

    if (target.matches('[data-action="open-delete-modal"]')) {
      openDeleteModal({
        id: target.getAttribute("data-id") || "",
        label: target.getAttribute("data-label") || ""
      });
      return;
    }

    if (target.matches('[data-action="close-delete-modal"]')) {
      closeDeleteModal();
      return;
    }

    if (target.matches('[data-action="open-billing-detail"]')) {
      openBillingDetail({
        label: target.getAttribute("data-label") || "-",
        status: target.getAttribute("data-status") || "-",
        required: target.getAttribute("data-required") || "-",
        paidUntil: target.getAttribute("data-paid-until") || "-",
        userCount: Number(target.getAttribute("data-user-count") || 0),
        monthlyFee: Number(target.getAttribute("data-monthly-fee") || 0),
        unpaidCount: Number(target.getAttribute("data-unpaid-count") || 0),
        totalDue: Number(target.getAttribute("data-total-due") || 0),
        unpaidMonths: target.getAttribute("data-unpaid-months") || "-"
      });
      var notifyBtn = $("billingNotifyBtn");
      if (notifyBtn) {
        notifyBtn.setAttribute("data-router-id", target.getAttribute("data-router-id") || "");
      }
      return;
    }

    if (target.matches('[data-action="close-billing-detail"]')) {
      closeBillingDetail();
      return;
    }

    if (target.matches('[data-action="send-billing-notify"]')) {
      sendBillingNotify(target.getAttribute("data-router-id") || "");
    }
  });

  initDarkMode();
  initNetStatus();
  initRouterBillingHeartbeat();
  initSubmitPace();
})();
