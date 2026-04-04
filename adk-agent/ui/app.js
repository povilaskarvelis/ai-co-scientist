const state = {
  conversations: [],
  selectedConversationId: null,
  selectedConversationDetail: null,
  selectedReportTaskId: null,
  selectedInputMode: "report",
  activeRunIds: new Set(),
  runsByRunId: {},
  runsByTaskId: {},
  pendingRunId: null,
  pollTimer: null,
  pollInFlight: false,
  isLoading: false,
  health: null,
  clarificationMessage: "",
  pendingUserMessage: "",
  exportingPdf: false,
  reportStatusTaskId: null,
  reportStatusText: "",
  reportStatusError: false,
  reportPanelMode: "report",
  renderedReportTaskId: "",
  renderedReportTitle: "",
  renderedReportMarkdown: "",
  debugOpen: false,
  debugByTaskId: {},
  graphByTaskId: {},
  visualsByTaskId: {},
  analysisWorkspaceByConversationId: {},
  graphSearchByTaskId: {},
  graphSelectionByTaskId: {},
  graphCy: null,
  graphRenderedTaskId: "",
  graphRenderedPayload: null,
  activityExpandedByTask: {},
  handlingTerminalRunIds: new Set(),
  startingTaskIds: new Set(),
  pendingAnalysisScrollTaskId: "",
};

const el = {
  workspace: document.getElementById("workspace"),
  tasksList: document.getElementById("tasksList"),
  taskTitle: document.getElementById("taskTitle"),
  messages: document.getElementById("messages"),
  promptInput: document.getElementById("promptInput"),
  sendBtn: document.getElementById("sendBtn"),
  composerForm: document.getElementById("composerForm"),
  newChatBtn: document.getElementById("newChatBtn"),
  notice: document.getElementById("notice"),
  modePicker: document.getElementById("modePicker"),
  exampleQueriesLabel: document.getElementById("exampleQueriesLabel"),
  reportPanel: document.getElementById("reportPanel"),
  reportTitle: document.getElementById("reportTitle"),
  reportStatus: document.getElementById("reportStatus"),
  datasetVisualsBtn: document.getElementById("datasetVisualsBtn"),
  evidenceGraphBtn: document.getElementById("evidenceGraphBtn"),
  reportContent: document.getElementById("reportContent"),
  visualsPanel: document.getElementById("visualsPanel"),
  graphPanel: document.getElementById("graphPanel"),
  graphSearchInput: document.getElementById("graphSearchInput"),
  graphFitBtn: document.getElementById("graphFitBtn"),
  graphResetBtn: document.getElementById("graphResetBtn"),
  graphLegend: document.getElementById("graphLegend"),
  graphCanvas: document.getElementById("graphCanvas"),
  graphInspector: document.getElementById("graphInspector"),
  debugToggleBtn: document.getElementById("debugToggleBtn"),
  debugPanel: document.getElementById("debugPanel"),
  debugSummary: document.getElementById("debugSummary"),
  debugJson: document.getElementById("debugJson"),
  exportPdfBtn: document.getElementById("exportPdfBtn"),
};

const TASK_MODE_LABELS = {
  report: "Biomedical Report",
  analysis: "Open Data Analysis",
};

const GRAPH_NODE_TYPE_COLORS = {
  objective: "#ffd166",
  source: "#8ec5ff",
  query_focus: "#ffcf70",
  support_cluster: "#4f627c",
  compound: "#7dd8ff",
  gene: "#6fb7ff",
  protein: "#8a93ff",
  disease: "#ff8b82",
  phenotype: "#ffb062",
  pathway: "#6be6be",
  trial: "#f7d96b",
  paper: "#d6a8ff",
  dataset: "#8ce085",
  tissue: "#59d8c8",
  cell_line: "#f5a7b8",
  study: "#c5d06f",
  literal: "#9ea7b3",
  record: "#9ea7b3",
};

const GRAPH_EDGE_COLORS = {
  default: "#8da2b8",
  mixed: "#f3b46f",
};

function normalizeTaskMode(value) {
  return String(value || "").trim().toLowerCase() === "analysis" ? "analysis" : "report";
}

function currentConversationMode() {
  const detail = state.selectedConversationDetail;
  const conversationMode = normalizeTaskMode(detail?.conversation?.mode || "");
  if (state.selectedConversationId && detail?.conversation?.conversation_id) {
    return conversationMode;
  }
  return normalizeTaskMode(state.selectedInputMode);
}

function formatDate(iso) {
  if (!iso) return "unknown";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function inlineMarkdown(text) {
  let value = text.replace(/<a\s+id="[^"]*">\s*<\/a>/gi, "");
  value = escapeHtml(value);
  value = value.replace(/\[([^\]]+)\]\((#[^)\s]+)\)/g, '<a href="$2">$1</a>');
  value = value.replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
  value = value.replace(/(^|[\s(])((?:https?:\/\/)[^\s<)]+)(?=$|[\s).,;:!?])/g, '$1<a href="$2" target="_blank" rel="noopener noreferrer">$2</a>');
  value = value.replace(/`([^`]+)`/g, "<code>$1</code>");
  value = value.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  value = value.replace(/\*([^*]+)\*/g, "<em>$1</em>");
  return value;
}

function markdownToHtml(markdown) {
  const lines = String(markdown || "").replace(/\r\n?/g, "\n").split("\n");
  const html = [];
  let inCode = false;
  let codeBuffer = [];
  let inBlockquote = false;
  const listStack = [];

  const closeCurrentList = () => {
    const current = listStack.pop();
    if (!current) return;
    if (current.liOpen) html.push("</li>");
    html.push(`</${current.type}>`);
  };
  const closeListsToIndent = (targetIndent = -1) => {
    while (listStack.length && listStack[listStack.length - 1].indent > targetIndent) {
      closeCurrentList();
    }
  };
  const closeLists = () => {
    closeListsToIndent(-1);
  };
  const closeBlockquote = () => {
    if (inBlockquote) { html.push("</blockquote>"); inBlockquote = false; }
  };
  const openList = (type, indent, start = 1) => {
    if (type === "ol") {
      html.push(Number.isFinite(start) && start > 1 ? `<ol start="${start}">` : "<ol>");
    } else {
      html.push("<ul>");
    }
    listStack.push({ type, indent, liOpen: false });
  };
  const renderListItem = ({ type, indent, content, anchorHtml: itemAnchorHtml, start = 1 }) => {
    closeBlockquote();
    closeListsToIndent(indent);

    let current = listStack[listStack.length - 1];
    if (!current || indent > current.indent) {
      openList(type, indent, start);
      current = listStack[listStack.length - 1];
    } else if (current.type !== type || current.indent !== indent) {
      closeCurrentList();
      openList(type, indent, start);
      current = listStack[listStack.length - 1];
    }

    if (current.liOpen) html.push("</li>");
    html.push("<li>");
    current.liOpen = true;
    html.push(`${itemAnchorHtml}${inlineMarkdown(content)}`);
  };
  const looksLikeTableRow = (value) => {
    const text = String(value || "").trim();
    if (!text.includes("|")) return false;
    const pipeCount = (text.match(/\|/g) || []).length;
    return pipeCount >= 2 || (pipeCount === 1 && text.startsWith("|"));
  };
  const isTableSeparator = (value) => {
    const text = String(value || "").trim();
    if (!looksLikeTableRow(text)) return false;
    const normalized = text.replace(/^\|/, "").replace(/\|$/, "");
    const cells = normalized.split("|").map((cell) => cell.trim());
    if (!cells.length) return false;
    return cells.every((cell) => /^:?-{3,}:?$/.test(cell));
  };
  const splitTableCells = (value) => {
    const text = String(value || "").trim().replace(/^\|/, "").replace(/\|$/, "");
    const cells = [];
    let current = "";
    let escaped = false;
    for (const ch of text) {
      if (escaped) {
        current += ch;
        escaped = false;
        continue;
      }
      if (ch === "\\") {
        escaped = true;
        continue;
      }
      if (ch === "|") {
        cells.push(current.trim());
        current = "";
        continue;
      }
      current += ch;
    }
    cells.push(current.trim());
    return cells;
  };
  const startsStructuralBlock = (value, nextValue = "") => {
    const text = String(value || "").trim();
    if (!text) return true;
    if (/^<a\s+id="[^"]+">\s*<\/a>/.test(text)) return true;
    if (text.startsWith("```")) return true;
    if (/^(?:---+|\*\*\*+|___+)$/.test(text)) return true;
    if (/^#{1,3}\s+/.test(text)) return true;
    if (/^>\s?/.test(text)) return true;
    if (/^(\s*)([-*]|\d+\.)\s+/.test(String(value || ""))) return true;
    if (looksLikeTableRow(text) && isTableSeparator(nextValue)) return true;
    return false;
  };

  for (let idx = 0; idx < lines.length; idx += 1) {
    const rawLine = lines[idx];
    const line = String(rawLine || "");
    let trimmed = line.trim();

    // Extract <a id="..."></a> anchors — re-emit as real HTML anchor targets
    let anchorHtml = "";
    const am = trimmed.match(/^<a\s+id="([^"]+)">\s*<\/a>/);
    if (am) {
      anchorHtml = `<span id="${escapeHtml(am[1])}"></span>`;
      trimmed = trimmed.slice(am[0].length).trim();
    }

    if (trimmed.startsWith("```")) {
      closeBlockquote();
      closeLists();
      if (inCode) {
        html.push(`<pre><code>${escapeHtml(codeBuffer.join("\n"))}</code></pre>`);
        codeBuffer = [];
        inCode = false;
      } else {
        inCode = true;
      }
      continue;
    }

    if (inCode) {
      codeBuffer.push(line);
      continue;
    }

    if (!trimmed) {
      closeBlockquote();
      closeLists();
      continue;
    }

    // Horizontal rule
    if (/^(?:---+|\*\*\*+|___+)$/.test(trimmed)) {
      closeBlockquote();
      closeLists();
      html.push("<hr>");
      continue;
    }

    const h3 = trimmed.match(/^###\s+(.+)$/);
    if (h3) {
      closeBlockquote();
      closeLists();
      html.push(`${anchorHtml}<h3>${inlineMarkdown(h3[1])}</h3>`);
      continue;
    }
    const h2 = trimmed.match(/^##\s+(.+)$/);
    if (h2) {
      closeBlockquote();
      closeLists();
      html.push(`${anchorHtml}<h2>${inlineMarkdown(h2[1])}</h2>`);
      continue;
    }
    const h1 = trimmed.match(/^#\s+(.+)$/);
    if (h1) {
      closeBlockquote();
      closeLists();
      html.push(`${anchorHtml}<h1>${inlineMarkdown(h1[1])}</h1>`);
      continue;
    }

    // Blockquotes — merge consecutive > lines into a single <blockquote>
    const quoteMatch = trimmed.match(/^>\s?(.*)$/);
    if (quoteMatch) {
      closeLists();
      if (!inBlockquote) {
        html.push("<blockquote>");
        inBlockquote = true;
      }
      const content = (quoteMatch[1] || "").trim();
      if (content) {
        html.push(`<p>${inlineMarkdown(content)}</p>`);
      }
      continue;
    }

    closeBlockquote();

    const nextLine = idx + 1 < lines.length ? String(lines[idx + 1] || "") : "";
    if (looksLikeTableRow(trimmed) && isTableSeparator(nextLine)) {
      closeLists();
      const headerCells = splitTableCells(trimmed);
      const rows = [];
      idx += 2;
      while (idx < lines.length) {
        const candidate = String(lines[idx] || "");
        const candidateTrimmed = candidate.trim();
        if (!candidateTrimmed || !looksLikeTableRow(candidateTrimmed) || isTableSeparator(candidateTrimmed)) {
          idx -= 1;
          break;
        }
        rows.push(splitTableCells(candidateTrimmed));
        idx += 1;
      }

      html.push("<table><thead><tr>");
      headerCells.forEach((cell) => {
        html.push(`<th>${inlineMarkdown(cell)}</th>`);
      });
      html.push("</tr></thead>");
      if (rows.length) {
        html.push("<tbody>");
        rows.forEach((row) => {
          html.push("<tr>");
          headerCells.forEach((_, cellIdx) => {
            html.push(`<td>${inlineMarkdown(row[cellIdx] || "")}</td>`);
          });
          html.push("</tr>");
        });
        html.push("</tbody>");
      }
      html.push("</table>");
      continue;
    }

    const listMatch = line.match(/^(\s*)([-*]|\d+\.)\s+(.+)$/);
    if (listMatch) {
      const indent = (listMatch[1] || "").replace(/\t/g, "  ").length;
      const marker = listMatch[2] || "";
      const content = listMatch[3] || "";
      const type = /^\d+\.$/.test(marker) ? "ol" : "ul";
      const start = type === "ol" ? Number.parseInt(marker, 10) || 1 : 1;
      renderListItem({ type, indent, content, anchorHtml, start });
      continue;
    }

    closeLists();
    const paragraphParts = [trimmed];
    while (idx + 1 < lines.length) {
      const candidateRaw = String(lines[idx + 1] || "");
      const candidateTrimmed = candidateRaw.trim();
      const candidateNext = idx + 2 < lines.length ? String(lines[idx + 2] || "") : "";
      if (!candidateTrimmed) break;
      if (startsStructuralBlock(candidateRaw, candidateNext)) break;
      paragraphParts.push(candidateTrimmed);
      idx += 1;
    }
    html.push(`${anchorHtml}<p>${inlineMarkdown(paragraphParts.join(" "))}</p>`);
  }

  closeBlockquote();
  closeLists();
  if (inCode) html.push(`<pre><code>${escapeHtml(codeBuffer.join("\n"))}</code></pre>`);
  return html.join("");
}

async function api(path, options = {}) {
  const config = {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  };
  const res = await fetch(path, config);
  const raw = await res.text();
  let parsed = null;
  try {
    parsed = raw ? JSON.parse(raw) : null;
  } catch {
    parsed = raw;
  }
  if (!res.ok) {
    const detail = (parsed && parsed.detail) || (typeof parsed === "string" ? parsed : `HTTP ${res.status}`);
    throw new Error(detail);
  }
  return parsed;
}

function setNotice(message = "", isError = false) {
  if (!message) {
    el.notice.classList.add("hidden");
    el.notice.textContent = "";
    return;
  }
  el.notice.classList.remove("hidden");
  el.notice.textContent = message;
  el.notice.style.borderColor = isError ? "#4c232c" : "#303030";
  el.notice.style.background = isError ? "#1a0c10" : "#121212";
  el.notice.style.color = isError ? "#ffccd6" : "#f1f1f1";
}

function setExpanded(expanded) {
  document.body.classList.toggle("expanded", !!expanded);
  updatePromptPlaceholder();
}

function updatePromptPlaceholder() {
  if (state.selectedConversationId) {
    el.promptInput.placeholder = "Type a follow-up...";
    return;
  }
  el.promptInput.placeholder = currentConversationMode() === "analysis"
    ? "Describe the open-data search or analysis task..."
    : "Ask any biomedical question...";
}

function renderModePicker() {
  const showModePicker = !state.selectedConversationId && !state.pendingUserMessage && !state.clarificationMessage;
  if (el.modePicker) {
    el.modePicker.classList.toggle("hidden", !showModePicker);
    el.modePicker.querySelectorAll("[data-mode]").forEach((card) => {
      const mode = normalizeTaskMode(card.dataset.mode || "");
      const isActive = mode === currentConversationMode();
      card.classList.toggle("active", isActive);
      card.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  }
  if (el.exampleQueriesLabel) {
    el.exampleQueriesLabel.textContent = currentConversationMode() === "analysis"
      ? "Example analysis prompts"
      : "Example report prompts";
  }
  const exampleContainer = document.getElementById("exampleQueries");
  if (exampleContainer) {
    exampleContainer.querySelectorAll(".example-query").forEach((chip) => {
      const chipMode = normalizeTaskMode(chip.dataset.mode || "");
      chip.classList.toggle("hidden", chipMode !== currentConversationMode());
    });
  }
}

function updateSendVisibility() {
  const hasText = el.promptInput.value.trim().length > 0;
  el.sendBtn.classList.toggle("hidden", !hasText);
  el.sendBtn.disabled = !hasText || state.isLoading || !state.health?.ok;
}

function setLoading(isLoading) {
  state.isLoading = !!isLoading;
  updateSendVisibility();
}

function setInnerHtmlIfChanged(element, html) {
  if (!element) return false;
  if (element.innerHTML === html) return false;
  element.innerHTML = html;
  return true;
}

function conversationTitle(conversation) {
  const text = String(conversation?.title || "").trim();
  return text || "Untitled Research";
}

function latestIteration(detail) {
  const iterations = Array.isArray(detail?.iterations) ? detail.iterations : [];
  if (!iterations.length) return null;
  return iterations[iterations.length - 1] || null;
}

function findIteration(detail, taskId) {
  if (!taskId) return null;
  const iterations = Array.isArray(detail?.iterations) ? detail.iterations : [];
  for (const iteration of iterations) {
    if (String(iteration?.task?.task_id || "") === String(taskId)) return iteration;
  }
  return null;
}

function latestCompletedTaskId(detail) {
  const iterations = Array.isArray(detail?.iterations) ? detail.iterations : [];
  for (let idx = iterations.length - 1; idx >= 0; idx -= 1) {
    const iteration = iterations[idx];
    if (String(iteration?.task?.status || "") === "completed") {
      return String(iteration?.task?.task_id || "");
    }
  }
  return "";
}

function planHtmlForIteration(iteration) {
  const activePlan = iteration?.active_plan_version;
  const task = iteration?.task || {};
  const steps = Array.isArray(activePlan?.steps)
    ? activePlan.steps
    : Array.isArray(task?.steps)
      ? task.steps
      : [];
  if (!steps.length) return "";

  let html = "<p>To answer your query I will:</p><ol class=\"plan-steps\">";
  steps.forEach((step, idx) => {
    const title = escapeHtml(String(step?.title || `Step ${idx + 1}`).trim());
    let source = String(step?.source || "").trim();
    let completion = String(step?.completion_condition || "").trim();

    // Fall back to parsing the instruction string if structured fields are absent
    if (!source || !completion) {
      const instruction = String(step?.instruction || "").trim();
      if (!source) {
        const m = instruction.match(/(?:Potential source|Source):\s*([^.]+)/i);
        if (m) source = m[1].trim();
      }
      if (!completion) {
        const m = instruction.match(/Done when:\s*(.+)$/i);
        if (m) completion = m[1].trim();
      }
    }

    html += `<li><span class="plan-step-title">${title}</span>`;
    if (source || completion) {
      html += `<ul class="plan-step-details">`;
      if (source) {
        html += `<li><span class="plan-step-label">Potential source</span>${escapeHtml(source)}</li>`;
      }
      if (completion) {
        html += `<li><span class="plan-step-label">Done when</span>${escapeHtml(completion)}</li>`;
      }
      html += `</ul>`;
    }
    html += `</li>`;
  });
  html += `</ol><p class="plan-followup">You can revise the plan, share suggestions, or continue when you're ready.</p>`;
  return html;
}

function checkpointHtml(taskId, planHtml, showAction, buttonLabel) {
  if (!planHtml && !showAction) return "";
  const normalizedTaskId = String(taskId || "").trim();
  const isStarting = state.startingTaskIds.has(normalizedTaskId);
  const action = showAction
    ? `<button class="primary-btn checkpoint-start-btn" data-action="checkpoint-start" data-task-id="${escapeHtml(normalizedTaskId)}" ${isStarting ? 'disabled aria-disabled="true"' : ""}>${escapeHtml(isStarting ? "Starting..." : (buttonLabel || "Start research"))}</button>`
    : "";
  return `
    <article class="message assistant checkpoint-message">
      <div class="message-body markdown-body">${planHtml}</div>
      ${action}
    </article>
  `;
}

function reportCardHtml(iteration) {
  const task = iteration?.task || {};
  const report = iteration?.report || {};
  const taskId = String(task.task_id || "");
  const mode = normalizeTaskMode(task.mode || report.mode || "");
  const hasAnalysisCells = Boolean(iteration?.analysis?.has_cells || task?.has_analysis_cells);
  if (!taskId) return "";
  if (mode === "analysis") {
    if (!hasAnalysisCells) return "";
    const isActive = String(state.selectedReportTaskId || "") === taskId;
    const branchLabel = String(iteration?.branch_label || "").trim();
    const cellCount = Number((iteration?.analysis?.cell_ids || []).length || 0);
    return `
      <article class="message assistant report-card ${isActive ? "active" : ""}" data-action="open-report" data-task-id="${escapeHtml(taskId)}" role="button" tabindex="0">
        <div class="report-card-head">
          <strong>Analysis ${Number(iteration?.iteration_index || 0) || 1}</strong>
          <span>${escapeHtml(formatDate(task.updated_at))}</span>
        </div>
        <div class="report-card-title">${escapeHtml(String(task.title || task.user_query || "Analysis step"))}</div>
        <div class="report-card-meta">${escapeHtml(`${cellCount} notebook cell${cellCount === 1 ? "" : "s"}`)}</div>
        ${branchLabel ? `<div class="branch-badge">${escapeHtml(branchLabel)}</div>` : ""}
      </article>
    `;
  }
  if (!report.has_report) return "";
  const isActive = String(state.selectedReportTaskId || "") === taskId;
  const branchLabel = String(iteration?.branch_label || "").trim();
  return `
    <article class="message assistant report-card ${isActive ? "active" : ""}" data-action="open-report" data-task-id="${escapeHtml(taskId)}" role="button" tabindex="0">
      <div class="report-card-head">
        <strong>Report ${Number(iteration?.iteration_index || 0) || 1}</strong>
        <span>${escapeHtml(formatDate(task.updated_at))}</span>
      </div>
      <div class="report-card-title">${escapeHtml(String(task.title || task.user_query || "Research report"))}</div>
      ${branchLabel ? `<div class="branch-badge">${escapeHtml(branchLabel)}</div>` : ""}
    </article>
  `;
}

function followUpSuggestionsHtml(iteration) {
  if (iteration?.is_direct_response) return "";
  const task = iteration?.task || {};
  if (normalizeTaskMode(task.mode || iteration?.report?.mode || "") === "analysis") return "";
  if (String(task.status || "") !== "completed") return "";
  const report = iteration?.report || {};
  if (!report.has_report) return "";
  const suggestions = Array.isArray(iteration?.follow_up_suggestions) ? iteration.follow_up_suggestions : [];
  const lines = ["What would you like to do next?"];
  lines.push("");
  lines.push("For example, we could:");
  if (suggestions.length) {
    for (const item of suggestions.slice(0, 3)) {
      lines.push(`- ${String(item || "").trim()}`);
    }
  } else {
    lines.push("- Ask a focused follow-up to deepen or stress-test the recommendation.");
  }
  return `
    <article class="message assistant">
      <div class="message-body markdown-body">${markdownToHtml(lines.join("\n"))}</div>
    </article>
  `;
}

function activityExpansionKey(taskId) {
  const value = String(taskId || "").trim();
  return value || "__pending__";
}

function isActivityExpanded(taskId) {
  return !!state.activityExpandedByTask[activityExpansionKey(taskId)];
}

function setActivityExpanded(taskId, expanded) {
  state.activityExpandedByTask[activityExpansionKey(taskId)] = !!expanded;
}

function normalizeActivityText(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function activityStepStatusLabel(status) {
  const normalized = String(status || "").trim();
  if (normalized === "completed") return "Completed";
  if (normalized === "blocked") return "Blocked";
  if (normalized === "in_progress") return "In progress";
  return "Pending";
}

function activityStepStatusClass(status) {
  const normalized = String(status || "").trim();
  if (normalized === "completed") return "is-complete";
  if (normalized === "blocked") return "is-blocked";
  if (normalized === "in_progress") return "is-running";
  return "";
}

function summarizeActivityToolEntry(entry) {
  const summary = normalizeActivityText(entry?.summary || "");
  const result = normalizeActivityText(entry?.result || "");
  const status = String(entry?.status || "done").trim();
  const tool = normalizeActivityText(entry?.tool || entry?.raw_tool || "Tool");
  if (summary && result) return `${summary} -> ${result}`;
  if (summary) return status === "called" ? `${summary}...` : summary;
  return `${tool} querying...`;
}

function collectActivitySources(step) {
  const sources = [];
  const addSource = (value) => {
    const normalized = normalizeActivityText(value);
    if (!normalized || sources.includes(normalized)) return;
    sources.push(normalized);
  };

  const dataSources = Array.isArray(step?.data_sources) ? step.data_sources : [];
  for (const source of dataSources) {
    addSource(source);
  }

  if (sources.length) return sources;

  addSource(step?.source || step?.tool_hint || "");

  const toolLog = Array.isArray(step?.tool_log) ? step.tool_log : [];
  for (const entry of toolLog) {
    addSource(entry?.tool || "");
  }

  return sources;
}

function renderActivitySectionHtml(label, items = [], extraClass = "") {
  const lines = (Array.isArray(items) ? items : []).filter(Boolean);
  if (!lines.length) return "";
  const classNames = ["activity-log-step-section", extraClass].filter(Boolean).join(" ");
  return `
    <div class="${classNames}">
      <div class="activity-log-step-label">${escapeHtml(label)}</div>
      <div class="activity-log-step-list">
        ${lines.map((line) => `<div class="activity-log-step-item">${inlineMarkdown(line)}</div>`).join("")}
      </div>
    </div>
  `;
}

function activityEventTimeLabel(iso) {
  if (!iso) return "";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function isNotebookActivityEvent(event) {
  const type = String(event?.type || event?.event_type || "").trim();
  const phase = String(event?.phase || "").trim();
  const tool = String(event?.metrics?.tool || "").trim();
  const author = String(event?.metrics?.author || "").trim();
  if (type === "notebook.tool.completed" || type === "notebook.diagnostic") return true;
  if (type === "synthesis.completed" && phase === "synthesize") return true;
  if (type === "tool.called" && author === "analysis_notebook_synthesizer") return true;
  if (type === "tool.failed" && (author === "analysis_notebook_synthesizer" || tool.startsWith("store_") || tool === "append_analysis_note")) return true;
  return false;
}

function notebookActivityEventLines(events = []) {
  const relevant = (Array.isArray(events) ? events : []).filter((event) => isNotebookActivityEvent(event));
  return relevant.slice(-12).map((event) => {
    const type = String(event?.type || event?.event_type || "").trim();
    const human = normalizeActivityText(event?.human_line || event?.type || "");
    let prefix = "Notebook";
    if (type === "tool.called") prefix = "Tool call";
    else if (type === "tool.failed") prefix = "Tool error";
    else if (type === "notebook.tool.completed") prefix = "Tool ok";
    else if (type === "notebook.diagnostic") prefix = "Decision";
    else if (type === "synthesis.completed") prefix = "Synthesis";
    return `${prefix}: ${human}`;
  });
}

function notebookActivitySummaryText(events = []) {
  const relevant = (Array.isArray(events) ? events : []).filter((event) => isNotebookActivityEvent(event));
  const latest = relevant.length ? relevant[relevant.length - 1] : null;
  return normalizeActivityText(latest?.human_line || latest?.type || "");
}

function notebookActivityIssueLines(events = []) {
  const relevant = (Array.isArray(events) ? events : []).filter((event) => isNotebookActivityEvent(event));
  return relevant
    .filter((event) => {
      const type = String(event?.type || event?.event_type || "").trim();
      const status = String(event?.status || "").trim();
      return status === "error" || type === "tool.failed";
    })
    .slice(-6)
    .map((event) => normalizeActivityText(event?.human_line || event?.type || ""))
    .filter(Boolean);
}

function buildActivityDetailsHtml(stepDetails = [], events = []) {
  const visibleSteps = (Array.isArray(stepDetails) ? stepDetails : [])
    .filter((step) => String(step?.status || "pending").trim() !== "pending");
  const notebookLines = notebookActivityEventLines(events);
  const notebookSummary = notebookActivitySummaryText(events);
  const notebookIssues = notebookActivityIssueLines(events);

  if (!visibleSteps.length && !notebookLines.length) {
    return '<div class="activity-log-empty">Activity details will appear here as steps begin.</div>';
  }

  return `
    <div class="activity-log">
      ${visibleSteps.map((step) => {
        const sid = String(step?.id || "").trim() || "Step";
        const status = String(step?.status || "pending").trim();
        const statusLabel = activityStepStatusLabel(status);
        const statusClass = activityStepStatusClass(status);
        const goal = normalizeActivityText(step?.goal || "") || sid;
        const sources = collectActivitySources(step);
        const toolLog = Array.isArray(step?.tool_log) ? step.tool_log : [];
        const toolLines = toolLog.map((entry) => summarizeActivityToolEntry(entry)).filter(Boolean);
        const resultSummary = normalizeActivityText(step?.result_summary || "");
        const openGaps = (Array.isArray(step?.open_gaps) ? step.open_gaps : [])
          .map((item) => normalizeActivityText(item))
          .filter(Boolean);
        const evidenceIds = (Array.isArray(step?.evidence_ids) ? step.evidence_ids : [])
          .map((item) => normalizeActivityText(item))
          .filter(Boolean);
        const summaryLines = [];
        if (resultSummary) summaryLines.push(resultSummary);
        if (!summaryLines.length) {
          if (status === "in_progress") {
            summaryLines.push(sources.length ? `Working with ${sources.join(", ")}.` : "Working on this step.");
          } else if (status === "blocked") {
            summaryLines.push("This step needs follow-up before it can continue.");
          } else {
            summaryLines.push("Step completed.");
          }
        }

        const metaParts = [statusLabel];
        if (sources.length) metaParts.push(`Sources: ${sources.join(", ")}`);

        return `
          <div class="activity-log-step ${statusClass}">
            <div class="activity-log-step-head">
              <span class="activity-log-step-dot" aria-hidden="true"></span>
              <div class="activity-log-step-copy">
                <div class="activity-log-step-title">
                  <span class="activity-log-step-id">${escapeHtml(sid)}</span>
                  <span class="activity-log-step-goal">${escapeHtml(goal)}</span>
                </div>
                <div class="activity-log-step-meta">${escapeHtml(metaParts.join(" · "))}</div>
              </div>
            </div>
            <div class="activity-log-step-body">
              ${renderActivitySectionHtml("Summary", summaryLines, "is-summary")}
              ${renderActivitySectionHtml("Activity", toolLines)}
              ${renderActivitySectionHtml("Evidence", evidenceIds)}
              ${renderActivitySectionHtml("Open questions", openGaps)}
            </div>
          </div>
        `;
      }).join("")}
      ${notebookLines.length ? `
        <div class="activity-log-step is-notebook">
          <div class="activity-log-step-head">
            <span class="activity-log-step-dot" aria-hidden="true"></span>
            <div class="activity-log-step-copy">
              <div class="activity-log-step-title">
                <span class="activity-log-step-id">Notebook</span>
                <span class="activity-log-step-goal">Code cell generation</span>
              </div>
              <div class="activity-log-step-meta">Notebook tool calls, plotting diagnostics, and rendering status</div>
            </div>
          </div>
          <div class="activity-log-step-body">
            ${renderActivitySectionHtml("Summary", notebookSummary ? [notebookSummary] : [], "is-summary")}
            ${renderActivitySectionHtml("Activity", notebookLines)}
            ${renderActivitySectionHtml("Issues", notebookIssues)}
          </div>
        </div>
      ` : ""}
    </div>
  `;
}

function reactTraceLines({ trace = "", phases = null } = {}) {
  const lines = [];
  const normalizedTrace = String(trace || "").trim();
  const phaseMap = phases && typeof phases === "object" ? phases : null;
  const order = ["reason", "act", "observe", "conclude"];
  const hasPhases = phaseMap && order.some((key) => String(phaseMap[key] || "").trim());
  if (!normalizedTrace && !hasPhases) return lines;

  lines.push("**Tool Trace**");
  lines.push("");

  if (hasPhases) {
    for (const key of order) {
      const value = String(phaseMap[key] || "").trim();
      if (!value) continue;
      const label = key.charAt(0).toUpperCase() + key.slice(1);
      lines.push(`- **${label}:** ${value}`);
    }
  } else {
    for (const rawLine of normalizedTrace.split("\n")) {
      const line = String(rawLine || "").trim();
      if (!line) continue;
      const m = line.match(/^(REASON|ACT|OBSERVE|CONCLUDE)\s*:\s*(.+)$/i);
      if (m) {
        const label = m[1].charAt(0).toUpperCase() + m[1].slice(1).toLowerCase();
        lines.push(`- **${label}:** ${m[2].trim()}`);
      } else {
        lines.push(`- ${line}`);
      }
    }
  }
  lines.push("");
  return lines;
}

function buildActivitySnapshot({ taskId = "", status = "", events = [], summaries = [], planApproved = true } = {}) {
  const normalizedStatus = String(status || "").trim();
  const safeEvents = (Array.isArray(events) ? events : []).filter((e) => String(e?.type || "") !== "plan.initializing");
  const safeSummaries = Array.isArray(summaries) ? summaries : [];
  const visibleStatuses = new Set(["running", "queued", "awaiting_hitl", "needs_clarification", "failed", "completed", "in_progress"]);
  const shouldShow = visibleStatuses.has(normalizedStatus) || safeEvents.length > 0 || safeSummaries.length > 0;
  if (!shouldShow) return null;

  const latestSummary = safeSummaries.length ? safeSummaries[safeSummaries.length - 1] : null;
  const latestEvent = safeEvents.length ? safeEvents[safeEvents.length - 1] : null;
  const latestLine = String(latestEvent?.human_line || latestEvent?.type || "").trim();

  const stepEvents = safeEvents.filter((e) => e?.type === "step.completed" && e?.metrics?.step_id);
  const toolCalledEvents = safeEvents.filter((e) => e?.type === "tool.called");
  const toolFailedEvents = safeEvents.filter((e) => e?.type === "tool.failed");
  const stepRetryEvents = safeEvents.filter((e) => e?.type === "step.retry");
  const stepStartedEvents = safeEvents.filter((e) => e?.type === "step.started" && e?.metrics?.step_id);
  const notebookEvents = safeEvents.filter((event) => isNotebookActivityEvent(event));
  const latestToolCalled = toolCalledEvents.length ? toolCalledEvents[toolCalledEvents.length - 1] : null;
  const latestToolFailed = toolFailedEvents.length ? toolFailedEvents[toolFailedEvents.length - 1] : null;
  const latestStepRetry = stepRetryEvents.length ? stepRetryEvents[stepRetryEvents.length - 1] : null;
  const latestStepStarted = stepStartedEvents.length ? stepStartedEvents[stepStartedEvents.length - 1] : null;
  const latestNotebookEvent = notebookEvents.length ? notebookEvents[notebookEvents.length - 1] : null;
  const stepsCompleted = Number(latestSummary?.steps_completed || stepEvents.length || 0);
  const stepsTotal = Number(latestSummary?.steps_total || 0);
  const stepDetails = Array.isArray(latestSummary?.step_details) ? latestSummary.step_details : [];

  const currentStepFromDetails = stepDetails.find((s) => String(s?.status || "") === "in_progress")
    || stepDetails.find((s) => String(s?.status || "") === "pending");

  const summaryByStatus = {
    queued: "Preparing research workflow.",
    running: "Preparing to execute the plan\u2026",
    in_progress: "Preparing to execute the plan\u2026",
    awaiting_hitl: "Waiting at a checkpoint for your input.",
    completed: "Research run completed.",
    failed: "Run ended with an error.",
    needs_clarification: "Clarification required to proceed.",
  };

  const title = "Research log";

  let summary = "";
  if (latestToolFailed) {
    summary = String(latestToolFailed?.human_line || "").trim() || "A tool call failed.";
  } else if (latestNotebookEvent && normalizedStatus === "completed") {
    summary = String(latestNotebookEvent?.human_line || "").trim() || "Notebook synthesis completed.";
  } else if (normalizedStatus === "completed" && (stepsTotal > 0 || latestSummary?.summary)) {
    summary = String(latestSummary?.summary || "").trim() || `${stepsCompleted}/${stepsTotal} plan steps executed`;
  } else if (latestStepRetry) {
    summary = String(latestStepRetry?.human_line || "").trim() || "Retrying the current step.";
  } else if (latestToolCalled) {
    summary = String(latestToolCalled?.human_line || "").trim() || "Querying sources\u2026";
  } else if (latestStepStarted) {
    summary = String(latestStepStarted?.human_line || "").trim() || "Executing step\u2026";
  } else if (currentStepFromDetails) {
    const sid = String(currentStepFromDetails?.id || "").trim();
    const goal = String(currentStepFromDetails?.goal || "").trim();
    const source = String(currentStepFromDetails?.source || currentStepFromDetails?.tool_hint || "").trim();
    summary = source ? `Querying ${source}\u2026` : (sid && goal ? `${sid}: ${goal}` : "Executing step\u2026");
  } else if (stepEvents.length > 0) {
    summary = String(latestSummary?.summary || "").trim() || summaryByStatus[normalizedStatus] || "Tracking workflow progress.";
  } else {
    summary = String(latestSummary?.summary || "").trim() || latestLine || summaryByStatus[normalizedStatus] || "Tracking workflow progress.";
  }

  let preview = "";
  const completedStepIds = new Set(stepEvents.map((e) => String(e?.metrics?.step_id || "").trim()).filter(Boolean));
  const stepPips = stepEvents.map((e) => {
    const sid = String(e?.metrics?.step_id || "").trim();
    const st = String(e?.metrics?.step_status || "completed").trim();
    const icon = st === "completed" ? "✓" : st === "blocked" ? "✗" : "…";
    return `${sid} ${icon}`;
  });
  const inProgressStepId = latestStepStarted && !completedStepIds.has(String(latestStepStarted?.metrics?.step_id || "").trim())
    ? String(latestStepStarted.metrics?.step_id || "").trim()
    : "";
  if (inProgressStepId) {
    stepPips.push(`${inProgressStepId} …`);
  }
  if (stepPips.length) {
    preview = stepPips.join("  ·  ");
    if (latestNotebookEvent && normalizedStatus === "completed") {
      preview = String(latestNotebookEvent?.human_line || "").trim() || preview;
    }
  } else if (latestLine) {
    preview = latestLine;
  } else if (normalizedStatus === "completed") {
    preview = "Execution finished. Expand to inspect details.";
  } else {
    preview = "Click for activity details";
  }

  return {
    taskId: String(taskId || "").trim() || "pending",
    status: normalizedStatus,
    title,
    summary,
    preview,
    detailsHtml: buildActivityDetailsHtml(stepDetails, safeEvents),
    planApproved: !!planApproved,
  };
}

function activityCardClassNames(status, expanded, planApproved = true) {
  const normalized = String(status || "").trim();
  const isActive = normalized === "running" || normalized === "in_progress" || normalized === "queued";
  const showSpinner = planApproved && isActive;
  return [
    "activity-card",
    expanded ? "expanded" : "",
    showSpinner ? "is-running" : "",
    normalized === "completed" ? "is-complete" : "",
    normalized === "failed" ? "is-error" : "",
    (normalized === "awaiting_hitl" || (isActive && !planApproved)) ? "is-paused" : "",
  ].filter(Boolean).join(" ");
}

function activityCardHtml(snapshot) {
  if (!snapshot) return "";
  const expanded = isActivityExpanded(snapshot.taskId);
  const status = String(snapshot.status || "").trim();
  const planApproved = snapshot.planApproved !== false;
  const classNames = activityCardClassNames(status, expanded, planApproved);

  return `
    <section
      class="${classNames}"
      data-role="activity-card"
      data-action="toggle-activity"
      data-task-id="${escapeHtml(snapshot.taskId)}"
      role="button"
      tabindex="0"
      aria-expanded="${expanded ? "true" : "false"}"
    >
      <div class="activity-head">
        <div class="activity-main">
          <span class="activity-wheel" aria-hidden="true"></span>
          <div class="activity-title-stack">
            <span class="activity-title">${escapeHtml(snapshot.title)}</span>
            <div class="activity-summary">${escapeHtml(snapshot.summary)}</div>
          </div>
        </div>
      </div>
      <div class="activity-preview">${escapeHtml(snapshot.preview)}</div>
      <div class="activity-details ${expanded ? "" : "hidden"}">${snapshot.detailsHtml}</div>
    </section>
  `;
}

function patchActivityCardElement(card, snapshot) {
  if (!card || !snapshot) return;
  const expanded = isActivityExpanded(snapshot.taskId);
  const planApproved = snapshot.planApproved !== false;
  const classNames = activityCardClassNames(snapshot.status, expanded, planApproved);
  if (card.className !== classNames) card.className = classNames;
  if (card.dataset.taskId !== snapshot.taskId) card.dataset.taskId = snapshot.taskId;
  const ariaExpanded = expanded ? "true" : "false";
  if (card.getAttribute("aria-expanded") !== ariaExpanded) card.setAttribute("aria-expanded", ariaExpanded);

  const titleEl = card.querySelector(".activity-title");
  if (titleEl && titleEl.textContent !== snapshot.title) titleEl.textContent = snapshot.title;
  const summaryEl = card.querySelector(".activity-summary");
  if (summaryEl && summaryEl.textContent !== snapshot.summary) summaryEl.textContent = snapshot.summary;
  const previewEl = card.querySelector(".activity-preview");
  if (previewEl && previewEl.textContent !== snapshot.preview) previewEl.textContent = snapshot.preview;
  const detailsEl = card.querySelector(".activity-details");
  if (detailsEl) {
    if (detailsEl.innerHTML !== snapshot.detailsHtml) detailsEl.innerHTML = snapshot.detailsHtml;
    detailsEl.classList.toggle("hidden", !expanded);
  }
}

function getRunForTask(taskId) {
  const tid = String(taskId || "").trim();
  return tid ? (state.runsByTaskId[tid] || null) : null;
}

const RESEARCH_EVENT_TYPES = new Set([
  "plan.generated", "plan.retry", "plan.failed", "task.created", "step.completed", "step.started", "step.retry", "tool.called", "tool.failed",
  "synthesis.completed", "checkpoint.opened", "execution.paused", "execution.running", "run.completed",
]);

function hasResearchProgress(run) {
  const evs = Array.isArray(run?.progress_events) ? run.progress_events : [];
  return evs.some((e) => RESEARCH_EVENT_TYPES.has(String(e?.type || "")));
}

function minimalLoadingSpinnerHtml(label = "") {
  const labelHtml = label ? `<span class="loading-label">${escapeHtml(label)}</span>` : "";
  return `<article class="message assistant loading-spinner-only" data-role="loading-spinner"><span class="activity-wheel" aria-hidden="true"></span>${labelHtml}</article>`;
}

function pendingRunLabel() {
  const run = state.pendingRunId ? (state.runsByRunId[state.pendingRunId] || state.runsByTaskId["__pending__"]) : null;
  const evs = Array.isArray(run?.progress_events) ? run.progress_events : [];
  if (evs.some((e) => String(e?.type || "") === "plan.initializing")) return "Preparing a plan\u2026";
  return "";
}

function updateLoadingSpinnerLabel() {
  if (!el.messages) return;
  const spinner = el.messages.querySelector('[data-role="loading-spinner"]');
  if (!spinner) return;
  const label = pendingRunLabel();
  let labelEl = spinner.querySelector(".loading-label");
  if (label) {
    if (!labelEl) {
      labelEl = document.createElement("span");
      labelEl.className = "loading-label";
      spinner.appendChild(labelEl);
    }
    labelEl.textContent = label;
  } else if (labelEl) {
    labelEl.remove();
  }
}

function iterationActivitySnapshot(iteration) {
  if (iteration?.is_direct_response) return null;
  const task = iteration?.task || {};
  const taskId = String(task.task_id || "").trim();
  if (!taskId) return null;
  if (state.startingTaskIds.has(taskId)) {
    return buildActivitySnapshot({
      taskId,
      status: "running",
      events: [{ type: "execution.running", human_line: "Executing plan..." }],
      summaries: [],
      planApproved: true,
    });
  }
  const run = getRunForTask(taskId);
  const taskStatus = String(task.status || "").trim();
  const runStatus = String(run?.status || "").trim();
  const hasActiveRun = run && ["running", "queued", "in_progress"].includes(runStatus);
  if (!hasActiveRun && !taskHasStarted(task) && taskStatus !== "completed" && taskStatus !== "failed") return null;
  const researchLog = iteration?.research_log || {};
  const runEvents = Array.isArray(run?.progress_events) ? run.progress_events : [];
  const runSummaries = Array.isArray(run?.progress_summaries) ? run.progress_summaries : [];
  const logEvents = Array.isArray(researchLog.events) ? researchLog.events : [];
  const logSummaries = Array.isArray(researchLog.summaries) ? researchLog.summaries : [];
  const status = String(run?.status || task.status || researchLog.status || "").trim();
  // For completed runs: prefer whichever source has more step completion data (handles edge cases
  // where run may be stale or research_log from persisted task has fuller data after refresh)
  const stepCount = (arr) => arr.filter((e) => e?.type === "step.completed" && e?.metrics?.step_id).length;
  const useLogForCompleted =
    (status === "completed" || status === "failed") &&
    stepCount(logEvents) > stepCount(runEvents);
  const events = useLogForCompleted ? logEvents : runEvents.length ? runEvents : logEvents;
  const summaries = useLogForCompleted ? logSummaries : runSummaries.length ? runSummaries : logSummaries;
  // Plan is approved if we have an active run executing, or task's hitl_history shows continue
  const planApproved = (run && ["running", "queued", "in_progress"].includes(status)) || taskHasStarted(task);
  return buildActivitySnapshot({ taskId, status, events, summaries, planApproved });
}

function pendingActivitySnapshot() {
  const run = state.pendingRunId ? (state.runsByRunId[state.pendingRunId] || state.runsByTaskId["__pending__"]) : null;
  const taskId = String(run?.task_id || "").trim() || "pending";
  const status = String(run?.status || "queued").trim();
  const events = Array.isArray(run?.progress_events) ? run.progress_events : [];
  const summaries = Array.isArray(run?.progress_summaries) ? run.progress_summaries : [];
  return buildActivitySnapshot({ taskId, status, events, summaries });
}

function taskHasStarted(task) {
  const history = Array.isArray(task?.hitl_history) ? task.hitl_history : [];
  return history.includes("approve") || history.includes("continue");
}

function placeActivityAfterPlan(task, activeRun) {
  const taskId = String(task?.task_id || "").trim();
  if (taskId && state.startingTaskIds.has(taskId)) return true;
  const status = String(activeRun?.status || task?.status || "").trim();
  // When we have an active run that's executing, plan was approved - place below immediately
  // (avoids delay until conversation is re-fetched with updated hitl_history)
  if (activeRun && ["running", "queued", "in_progress"].includes(status)) return true;
  // When plan hasn't been approved/started, always show research log above the plan
  if (!taskHasStarted(task)) return false;
  if (["running", "queued", "in_progress", "completed", "failed", "needs_clarification"].includes(status)) return true;
  if (status === "awaiting_hitl") return true;
  return false;
}

function updateInlineActivityCard(run) {
  if (!run || !el.messages) return;
  const runTaskId = String(run.task_id || "").trim();
  const snapshot = buildActivitySnapshot({
    taskId: runTaskId || "pending",
    status: String(run.status || "").trim(),
    events: Array.isArray(run.progress_events) ? run.progress_events : [],
    summaries: Array.isArray(run.progress_summaries) ? run.progress_summaries : [],
  });
  if (!snapshot) return;

  const cards = Array.from(el.messages.querySelectorAll('[data-role="activity-card"]'));
  if (!cards.length) return;
  const targetCards = cards.filter((card) => {
    const cardTaskId = String(card.dataset.taskId || "").trim();
    if (runTaskId) return cardTaskId === runTaskId || cardTaskId === "pending";
    return cardTaskId === "pending";
  });
  if (!targetCards.length) return;

  if (runTaskId && isActivityExpanded("pending") && !isActivityExpanded(runTaskId)) {
    setActivityExpanded(runTaskId, true);
  }

  for (const card of targetCards) {
    patchActivityCardElement(card, snapshot);
  }
}

function renderSidebar() {
  let html = "";
  if (!state.conversations.length) {
    html = '<p class="muted">No chats yet.</p>';
    setInnerHtmlIfChanged(el.tasksList, html);
    return;
  }
  html = state.conversations
    .map((conversation) => {
      const conversationId = String(conversation.conversation_id || "");
      const active = conversationId === state.selectedConversationId ? "active" : "";
      const title = escapeHtml(conversationTitle(conversation));
      const status = escapeHtml(String(conversation.latest_status || "unknown"));
      const count = Number(conversation.iteration_count || 0);
      return `
        <article class="task-item ${active}" data-conversation-id="${escapeHtml(conversationId)}">
          <div class="task-line">
            <span><span class="status-dot status-${status.replace(/\s+/g, "_")}"></span>${status}</span>
            <span>${formatDate(conversation.updated_at)}</span>
          </div>
          <div class="task-objective">${title}</div>
          <div class="task-line"><span>${count} iteration${count === 1 ? "" : "s"}</span></div>
        </article>
      `;
    })
    .join("");
  setInnerHtmlIfChanged(el.tasksList, html);
}

function renderTaskHeader() {
  const conversation = state.selectedConversationDetail?.conversation;
  if (!conversation) {
    if (el.taskTitle.textContent !== "New chat") el.taskTitle.textContent = "New chat";
    return;
  }
  const title = conversationTitle(conversation);
  const modeLabel = TASK_MODE_LABELS[normalizeTaskMode(conversation.mode)] || TASK_MODE_LABELS.report;
  const status = String(conversation.latest_status || "");
  const count = Number(conversation.iteration_count || 0);
  const nextTitle = `${title} · ${modeLabel} · ${status} · ${count} iteration${count === 1 ? "" : "s"}`;
  if (el.taskTitle.textContent !== nextTitle) el.taskTitle.textContent = nextTitle;
}

function renderMessages() {
  const detail = state.selectedConversationDetail;

  if (!detail && state.clarificationMessage) {
    const parts = [];
    if (state.pendingUserMessage) {
      parts.push(`<article class="message user"><pre class="message-body">${escapeHtml(state.pendingUserMessage)}</pre></article>`);
      const pendingRun = state.pendingRunId ? (state.runsByRunId[state.pendingRunId] || state.runsByTaskId["__pending__"]) : null;
      if (pendingRun && hasResearchProgress(pendingRun)) {
        const pendingCard = activityCardHtml(pendingActivitySnapshot());
        if (pendingCard) parts.push(pendingCard);
      } else {
        parts.push(minimalLoadingSpinnerHtml(pendingRunLabel()));
      }
    }
    parts.push(`<article class="message assistant"><div class="message-body markdown-body">${markdownToHtml(state.clarificationMessage)}</div></article>`);
    if (setInnerHtmlIfChanged(el.messages, parts.join(""))) {
      el.messages.scrollTop = el.messages.scrollHeight;
    }
    return;
  }

  if (!detail) {
    if (state.pendingUserMessage) {
      const parts = [
        `<article class="message user"><pre class="message-body">${escapeHtml(state.pendingUserMessage)}</pre></article>`,
      ];
      const pendingRun = state.pendingRunId ? (state.runsByRunId[state.pendingRunId] || state.runsByTaskId["__pending__"]) : null;
      if (pendingRun && hasResearchProgress(pendingRun)) {
        const pendingCard = activityCardHtml(pendingActivitySnapshot());
        if (pendingCard) parts.push(pendingCard);
      } else {
        parts.push(minimalLoadingSpinnerHtml(pendingRunLabel()));
      }
      if (setInnerHtmlIfChanged(el.messages, parts.join(""))) {
        el.messages.scrollTop = el.messages.scrollHeight;
      }
      return;
    }
    setInnerHtmlIfChanged(el.messages, "");
    return;
  }

  const iterations = Array.isArray(detail.iterations) ? detail.iterations : [];
  const parts = [];

  for (const iteration of iterations) {
    const task = iteration?.task || {};
    const userText = String(task.user_query || task.objective || "").trim() || "(empty query)";
    parts.push(`<article class="message user"><pre class="message-body">${escapeHtml(userText)}</pre></article>`);

    if (iteration?.is_direct_response) {
      const directText = String(iteration?.direct_response_text || "").trim();
      if (directText) {
        parts.push(`<article class="message assistant"><div class="message-body markdown-body">${markdownToHtml(directText)}</div></article>`);
      }
      continue;
    }

    const runForTask = getRunForTask(task.task_id);
    const activityCard = activityCardHtml(iterationActivitySnapshot(iteration));
    const shouldPlaceAfterPlan = placeActivityAfterPlan(task, runForTask);
    if (activityCard && !shouldPlaceAfterPlan) parts.push(activityCard);

    const branchLabel = String(iteration?.branch_label || "").trim();
    if (branchLabel) {
      parts.push(
        `<article class="message assistant"><div class="message-body"><span class="branch-badge">${escapeHtml(branchLabel)}</span></div></article>`
      );
    }

    const planHtml = planHtmlForIteration(iteration);
    const awaiting = Boolean(task.awaiting_hitl);
    const buttonLabel = task.hitl_history && (task.hitl_history.includes("approve") || task.hitl_history.includes("continue")) ? "Approve plan" : "Start research";
    parts.push(checkpointHtml(task.task_id, planHtml, awaiting, buttonLabel));
    if (activityCard && shouldPlaceAfterPlan) parts.push(activityCard);

    const reportCard = reportCardHtml(iteration);
    if (reportCard) parts.push(reportCard);

    const suggestions = followUpSuggestionsHtml(iteration);
    if (suggestions) parts.push(suggestions);
  }

  if (state.pendingUserMessage) {
    parts.push(`<article class="message user"><pre class="message-body">${escapeHtml(state.pendingUserMessage)}</pre></article>`);
    const pendingRun = state.pendingRunId ? (state.runsByRunId[state.pendingRunId] || state.runsByTaskId["__pending__"]) : null;
    if (pendingRun && hasResearchProgress(pendingRun)) {
      const pendingCard = activityCardHtml(pendingActivitySnapshot());
      if (pendingCard) parts.push(pendingCard);
    } else {
      parts.push(minimalLoadingSpinnerHtml(pendingRunLabel()));
    }
  }

  if (setInnerHtmlIfChanged(el.messages, parts.join(""))) {
    el.messages.scrollTop = el.messages.scrollHeight;
  }
}

function setReportStatus(taskId, message = "", isError = false) {
  state.reportStatusTaskId = taskId || null;
  state.reportStatusText = String(message || "");
  state.reportStatusError = !!isError;
}

function currentReportIteration() {
  const detail = state.selectedConversationDetail;
  if (!detail) return null;
  const explicit = findIteration(detail, state.selectedReportTaskId);
  if (explicit) return explicit;
  const selected = String(detail?.conversation?.selected_report_task_id || "").trim();
  if (selected) return findIteration(detail, selected);
  const latestDone = latestCompletedTaskId(detail);
  if (latestDone) return findIteration(detail, latestDone);
  return latestIteration(detail);
}

function currentDebugTaskId() {
  const iteration = currentReportIteration();
  return String(iteration?.task?.task_id || "").trim();
}

function currentGraphTaskId() {
  return currentDebugTaskId();
}

function currentVisualsTaskId() {
  return currentDebugTaskId();
}

function currentAnalysisWorkspaceEntry() {
  const conversationId = String(state.selectedConversationId || "").trim();
  if (!conversationId) return null;
  return state.analysisWorkspaceByConversationId[conversationId] || null;
}

function currentAnalysisWorkspace() {
  return currentAnalysisWorkspaceEntry()?.data?.workspace || null;
}

function analysisCellById(workspace, cellId) {
  const cells = Array.isArray(workspace?.cells) ? workspace.cells : [];
  return cells.find((cell) => String(cell?.cell_id || "") === String(cellId || "")) || null;
}

function analysisFirstCellForTask(workspace, taskId) {
  const cells = Array.isArray(workspace?.cells) ? workspace.cells : [];
  return cells.find((cell) => String(cell?.task_id || "") === String(taskId || "")) || null;
}

function destroyGraphInstance() {
  if (state.graphCy && typeof state.graphCy.destroy === "function") {
    state.graphCy.destroy();
  }
  state.graphCy = null;
  state.graphRenderedTaskId = "";
  state.graphRenderedPayload = null;
  if (el.graphCanvas) {
    el.graphCanvas.innerHTML = "";
    el.graphCanvas.className = "graph-canvas";
  }
  if (el.graphInspector) {
    el.graphInspector.innerHTML = "";
  }
}

function clearRenderedReportPanel() {
  state.renderedReportTaskId = "";
  state.renderedReportTitle = "";
  state.renderedReportMarkdown = "";
  destroyGraphInstance();
  el.reportTitle.textContent = "Research Report";
  el.reportContent.innerHTML = "";
  if (el.visualsPanel) el.visualsPanel.innerHTML = "";
}

function setRenderedReportTitle(taskId, title) {
  const normalizedTaskId = String(taskId || "").trim();
  const normalizedTitle = String(title || "");
  if (
    state.renderedReportTaskId === normalizedTaskId
    && state.renderedReportTitle === normalizedTitle
  ) {
    return;
  }
  el.reportTitle.textContent = normalizedTitle || "Research Report";
  state.renderedReportTaskId = normalizedTaskId;
  state.renderedReportTitle = normalizedTitle;
}

function setRenderedReportMarkdown(taskId, markdown, { preserveScroll = true } = {}) {
  const normalizedTaskId = String(taskId || "").trim();
  const normalizedMarkdown = String(markdown || "");
  const sameTask = preserveScroll && state.renderedReportTaskId === normalizedTaskId;
  if (sameTask && state.renderedReportMarkdown === normalizedMarkdown) return;
  const scrollTop = sameTask ? el.reportContent.scrollTop : 0;
  const scrollLeft = sameTask ? el.reportContent.scrollLeft : 0;
  el.reportContent.innerHTML = markdownToHtml(normalizedMarkdown);
  state.renderedReportTaskId = normalizedTaskId;
  state.renderedReportMarkdown = normalizedMarkdown;
  if (sameTask) {
    el.reportContent.scrollTop = scrollTop;
    el.reportContent.scrollLeft = scrollLeft;
  }
}

function setDebugSummaryHtml(taskId, html) {
  const normalizedTaskId = String(taskId || "").trim();
  if (
    String(el.debugSummary.dataset.taskId || "") === normalizedTaskId
    && el.debugSummary.innerHTML === html
  ) {
    return;
  }
  el.debugSummary.innerHTML = html;
  el.debugSummary.dataset.taskId = normalizedTaskId;
}

function setDebugJsonText(taskId, text) {
  const normalizedTaskId = String(taskId || "").trim();
  const sameTask = String(el.debugJson.dataset.taskId || "") === normalizedTaskId;
  if (sameTask && el.debugJson.textContent === text) return;
  const scrollTop = sameTask ? el.debugJson.scrollTop : 0;
  const scrollLeft = sameTask ? el.debugJson.scrollLeft : 0;
  el.debugJson.textContent = text;
  el.debugJson.dataset.taskId = normalizedTaskId;
  if (sameTask) {
    el.debugJson.scrollTop = scrollTop;
    el.debugJson.scrollLeft = scrollLeft;
  }
}

function countCollectionItems(value) {
  if (Array.isArray(value)) return value.length;
  if (value && typeof value === "object") return Object.keys(value).length;
  return 0;
}

function escapePreformattedJson(value) {
  return escapeHtml(JSON.stringify(value || {}, null, 2));
}

function graphTypeColor(type) {
  return GRAPH_NODE_TYPE_COLORS[String(type || "").trim()] || GRAPH_NODE_TYPE_COLORS.record;
}

function graphEdgeColor(mixed = false) {
  return mixed ? GRAPH_EDGE_COLORS.mixed : GRAPH_EDGE_COLORS.default;
}

function graphLegendHtml(payload = null) {
  const nodes = Array.isArray(payload?.elements?.nodes) ? payload.elements.nodes : [];
  const hiddenNodeTypes = new Set(["query_focus", "objective", "source", "support_cluster"]);
  const orderedVisibleTypes = [
    "compound",
    "gene",
    "protein",
    "disease",
    "phenotype",
    "trial",
    "paper",
    "dataset",
    "pathway",
    "tissue",
    "cell_line",
    "study",
    "literal",
    "record",
  ];
  const presentTypes = [...new Set(
    nodes
      .map((node) => String(node?.data?.type || "").trim())
      .filter((type) => type && !hiddenNodeTypes.has(type))
  )];
  const nodeTypes = presentTypes.length
    ? [
      ...orderedVisibleTypes.filter((type) => presentTypes.includes(type)),
      ...presentTypes.filter((type) => !orderedVisibleTypes.includes(type)).sort(),
    ]
    : ["compound", "gene", "disease", "trial", "paper", "dataset"];
  return [
    ...nodeTypes.map((type) => `
      <span class="graph-legend-item">
        <span class="graph-legend-swatch" style="background:${graphTypeColor(type)}"></span>
        ${escapeHtml(type.replaceAll("_", " "))}
      </span>
    `),
  ].join("");
}

function renderGraphCanvasState(kind, title, copy) {
  if (!el.graphCanvas) return;
  el.graphCanvas.className = `graph-canvas is-${kind}`;
  el.graphCanvas.innerHTML = `
    <div class="graph-empty-state">
      <h4 class="graph-empty-title">${escapeHtml(title)}</h4>
      <p class="graph-empty-copy">${escapeHtml(copy)}</p>
    </div>
  `;
}

function graphEmptyStateContent(payload) {
  const mode = String(payload?.mode || "semantic").trim();
  if (mode === "support_topology") {
    return {
      title: "No evidence topology yet",
      copy: "This session has not captured graphable evidence yet.",
    };
  }
  return {
    title: "No semantic claims yet",
    copy: "This session does not currently contain graphable semantic claims.",
  };
}

function summarizeGraphWarnings(warnings = []) {
  const lines = (Array.isArray(warnings) ? warnings : []).map((item) => String(item || "").trim()).filter(Boolean);
  if (!lines.length) return "";
  return `
    <div class="graph-inspector-section">
      <h4>Notes</h4>
      <div class="graph-chip-list">
        ${lines.map((line) => `<span class="graph-chip is-warn">${escapeHtml(line)}</span>`).join("")}
      </div>
    </div>
  `;
}

function graphSummaryMetrics(payload) {
  const summary = payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
  const elements = payload?.elements && typeof payload.elements === "object" ? payload.elements : {};
  const nodes = Array.isArray(elements.nodes) ? elements.nodes : [];
  const edges = Array.isArray(elements.edges) ? elements.edges : [];
  const mode = String(payload?.mode || "semantic").trim();
  const nodeCount = Number(summary.node_count || nodes.length || 0);
  const edgeCount = Number(summary.edge_count || edges.length || 0);
  if (mode === "support_topology") {
    return [
      { label: "Nodes", value: String(nodeCount) },
      { label: "Links", value: String(edgeCount) },
    ];
  }
  return [
    { label: "Nodes", value: String(nodeCount) },
    { label: "Claims", value: String(edgeCount) },
  ];
}

function graphSummaryInspectorHtml(payload) {
  const summary = payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
  const warnings = Array.isArray(payload?.warnings) ? payload.warnings : [];
  const mode = String(payload?.mode || "semantic").trim();
  const metrics = graphSummaryMetrics(payload);
  const description = mode === "support_topology"
    ? "This report did not capture structured semantic claims, so the graph is showing support topology for any captured evidence in the session."
    : "Select a node or edge to inspect the underlying evidence. This view hides workflow scaffolding and focuses on semantic claims.";
  return `
    <div class="graph-inspector-card">
      <div>
        <div class="graph-inspector-kicker">Evidence Graph</div>
        <h4 class="graph-inspector-title">Session overview</h4>
      </div>
      <p class="graph-inspector-copy">${escapeHtml(description)}</p>
      <div class="graph-inspector-grid">
        ${metrics.map((item) => `
          <div class="graph-inspector-metric">
            <span class="graph-inspector-label">${escapeHtml(item.label)}</span>
            <span class="graph-inspector-value">${escapeHtml(item.value)}</span>
          </div>
        `).join("")}
      </div>
      ${summarizeGraphWarnings(warnings)}
    </div>
  `;
}

function graphMetricHtml(items = []) {
  const metrics = (Array.isArray(items) ? items : []).filter((item) => item && String(item.value || "").trim() !== "");
  if (!metrics.length) return "";
  return `
    <div class="graph-inspector-grid">
      ${metrics.map((item) => `
        <div class="graph-inspector-metric">
          <span class="graph-inspector-label">${escapeHtml(item.label)}</span>
          <span class="graph-inspector-value">${escapeHtml(item.value)}</span>
        </div>
      `).join("")}
    </div>
  `;
}

function graphChipListHtml(items = [], { warn = false } = {}) {
  const lines = (Array.isArray(items) ? items : []).map((item) => String(item || "").trim()).filter(Boolean);
  if (!lines.length) return "";
  return `
    <div class="graph-chip-list">
      ${lines.map((line) => `<span class="graph-chip ${warn ? "is-warn" : ""}">${escapeHtml(line)}</span>`).join("")}
    </div>
  `;
}

function graphAttrsToLines(attrs) {
  const payload = attrs && typeof attrs === "object" ? attrs : {};
  return Object.entries(payload)
    .map(([key, value]) => {
      if (value == null || value === "" || (Array.isArray(value) && !value.length)) return "";
      const rendered = Array.isArray(value) ? value.join(", ") : (typeof value === "object" ? JSON.stringify(value) : String(value));
      return `${key.replaceAll("_", " ")}: ${rendered}`;
    })
    .filter(Boolean)
    .slice(0, 8);
}

function graphSelectionForTask(taskId) {
  return state.graphSelectionByTaskId[String(taskId || "").trim()] || null;
}

function setGraphSelection(taskId, selection) {
  const normalizedTaskId = String(taskId || "").trim();
  if (!normalizedTaskId) return;
  if (selection) state.graphSelectionByTaskId[normalizedTaskId] = selection;
  else delete state.graphSelectionByTaskId[normalizedTaskId];
}

function graphNeighborItems(cy, nodeId) {
  if (!cy || !nodeId) return [];
  return cy.getElementById(nodeId)
    .connectedEdges()
    .map((edge) => edge.data())
    .sort((left, right) => (
      Number(right.evidence_count || 0) - Number(left.evidence_count || 0)
      || ((right.primary_sources?.length || 0) - (left.primary_sources?.length || 0))
      || String(left.statement || left.label || "").localeCompare(String(right.statement || right.label || ""))
    ))
    .slice(0, 8)
    .map((data) => ({
      title: data.statement || data.label || "Connected claim",
      body: [
        `${Number(data.evidence_count || 0)} record${Number(data.evidence_count || 0) === 1 ? "" : "s"}`,
        data.primary_sources?.length ? data.primary_sources.slice(0, 2).join(", ") : "",
      ].filter(Boolean).join(" | "),
    }));
}

function graphNodeInspectorHtml(taskId, nodeData) {
  const aliases = Array.isArray(nodeData?.aliases) ? nodeData.aliases : [];
  const attrLines = graphAttrsToLines(nodeData?.attrs);
  const neighborLines = graphNeighborItems(state.graphCy, nodeData?.id);
  return `
    <div class="graph-inspector-card">
      <div>
        <div class="graph-inspector-kicker">Node</div>
        <h4 class="graph-inspector-title">${escapeHtml(String(nodeData?.full_label || nodeData?.label || "Node"))}</h4>
      </div>
      ${graphMetricHtml([
        { label: "Type", value: String(nodeData?.type || "record").replaceAll("_", " ") },
        { label: "Neighbors", value: String(nodeData?.degree || 0) },
        { label: "Connections", value: String(nodeData?.connected_claim_count || 0) },
      ])}
      ${aliases.length ? `
        <div class="graph-inspector-section">
          <h4>Aliases</h4>
          ${graphChipListHtml(aliases)}
        </div>
      ` : ""}
      ${attrLines.length ? `
        <div class="graph-inspector-section">
          <h4>Attributes</h4>
          ${graphChipListHtml(attrLines)}
        </div>
      ` : ""}
      ${neighborLines.length ? `
        <div class="graph-inspector-section">
          <h4>Connected relationships</h4>
          <div class="graph-inspector-list">
            ${neighborLines.map((item) => `
              <div class="graph-inspector-list-item">
                <p><strong>${escapeHtml(item.title)}</strong></p>
                <p>${escapeHtml(item.body)}</p>
              </div>
            `).join("")}
          </div>
        </div>
      ` : ""}
      ${summarizeGraphWarnings([])}
    </div>
  `;
}

function graphEdgeInspectorHtml(edgeData) {
  const sources = Array.isArray(edgeData?.primary_sources) ? edgeData.primary_sources : [];
  const ids = Array.isArray(edgeData?.supporting_ids) ? edgeData.supporting_ids : [];
  const qualifiers = Array.isArray(edgeData?.qualifiers) ? edgeData.qualifiers : [];
  return `
    <div class="graph-inspector-card">
      <div>
        <div class="graph-inspector-kicker">Relationship</div>
        <h4 class="graph-inspector-title">${escapeHtml(String(edgeData?.statement || edgeData?.label || "Relationship"))}</h4>
      </div>
      <p class="graph-inspector-copy">Predicate: ${escapeHtml(String(edgeData?.predicate || edgeData?.label || "related_to").replaceAll("_", " "))}</p>
      ${graphMetricHtml([
        { label: "Confidence", value: String(edgeData?.confidence || "unknown") },
        { label: "Records", value: String(edgeData?.evidence_count || 0) },
        { label: "Sources", value: String((Array.isArray(edgeData?.primary_sources) ? edgeData.primary_sources.length : 0)) },
      ])}
      ${sources.length ? `
        <div class="graph-inspector-section">
          <h4>Primary sources</h4>
          ${graphChipListHtml(sources)}
        </div>
      ` : ""}
      ${ids.length ? `
        <div class="graph-inspector-section">
          <h4>Supporting identifiers</h4>
          ${graphChipListHtml(ids)}
        </div>
      ` : ""}
      ${qualifiers.length ? `
        <div class="graph-inspector-section">
          <h4>Qualifiers</h4>
          ${graphChipListHtml(qualifiers)}
        </div>
      ` : ""}
      ${edgeData?.mixed_evidence ? `
        <div class="graph-inspector-section">
          <h4>Mixed evidence</h4>
          ${graphChipListHtml(["Conflicting evidence exists for this relationship."], { warn: true })}
        </div>
      ` : ""}
    </div>
  `;
}

function updateGraphInspector(taskId) {
  if (!el.graphInspector) return;
  const taskGraph = state.graphByTaskId[String(taskId || "").trim()]?.data || null;
  const selection = graphSelectionForTask(taskId);
  if (!selection || !taskGraph) {
    el.graphInspector.innerHTML = graphSummaryInspectorHtml(taskGraph || {});
    return;
  }
  if (selection.kind === "node") {
    el.graphInspector.innerHTML = graphNodeInspectorHtml(taskId, selection.data || {});
    return;
  }
  if (selection.kind === "edge") {
    el.graphInspector.innerHTML = graphEdgeInspectorHtml(selection.data || {});
    return;
  }
  el.graphInspector.innerHTML = graphSummaryInspectorHtml(taskGraph || {});
}

function buildGraphElements(payload) {
  const elements = payload?.elements && typeof payload.elements === "object" ? payload.elements : {};
  const nodes = (Array.isArray(elements.nodes) ? elements.nodes : []).map((node) => {
    const data = node?.data && typeof node.data === "object" ? { ...node.data } : {};
    const degree = Number(data.degree || 0);
    const connectedClaims = Number(data.connected_claim_count || 0);
    data.is_focus = Number(data.is_focus || 0) > 0 ? 1 : 0;
    data.focus_rank = Number(data.focus_rank || 0);
    data.focus_score = Number(data.focus_score || 0);
    data.support_weight = Number(data.support_weight || 0);
    data.color = graphTypeColor(data.type);
    data.size = Math.max(20, Math.min(44, 20 + (degree * 2.2) + (connectedClaims * 1.1) + (data.is_focus ? 6 : 0)));
    return { data };
  });
  const edges = (Array.isArray(elements.edges) ? elements.edges : []).map((edge) => {
    const data = edge?.data && typeof edge.data === "object" ? { ...edge.data } : {};
    const evidenceCount = Number(data.evidence_count || 0);
    const sourceCount = Array.isArray(data.primary_sources) ? data.primary_sources.length : 0;
    data.color = graphEdgeColor(data.mixed_evidence);
    data.width = Math.max(2.4, Math.min(6.8, 2.4 + (evidenceCount * 0.5) + (sourceCount * 0.2)));
    data.is_mixed = data.mixed_evidence ? 1 : 0;
    return { data };
  });
  return { nodes, edges };
}

function graphFocusNodeIds(payload, graphElements) {
  const summary = payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
  const fromSummary = Array.isArray(summary.focus_node_ids) ? summary.focus_node_ids : [];
  const normalized = fromSummary.map((value) => String(value || "").trim()).filter(Boolean);
  if (normalized.length) return normalized;
  return (Array.isArray(graphElements?.nodes) ? graphElements.nodes : [])
    .map((node) => node?.data && typeof node.data === "object" ? node.data : null)
    .filter((data) => data && Number(data.is_focus || 0) > 0)
    .map((data) => String(data.id || "").trim())
    .filter(Boolean);
}

function computeGraphFocusDistances(cy, focusIds) {
  const distances = {};
  const queue = [];
  const seen = new Set();
  for (const rawId of focusIds) {
    const nodeId = String(rawId || "").trim();
    if (!nodeId || seen.has(nodeId)) continue;
    const node = cy.getElementById(nodeId);
    if (!node || node.empty() || !node.isNode()) continue;
    distances[nodeId] = 0;
    queue.push(nodeId);
    seen.add(nodeId);
  }
  while (queue.length) {
    const currentId = queue.shift();
    const currentNode = cy.getElementById(currentId);
    const currentDistance = Number(distances[currentId] || 0);
    currentNode.connectedEdges().forEach((edge) => {
      edge.connectedNodes().forEach((neighbor) => {
        const neighborId = String(neighbor.id() || "").trim();
        if (!neighborId || seen.has(neighborId)) return;
        seen.add(neighborId);
        distances[neighborId] = currentDistance + 1;
        queue.push(neighborId);
      });
    });
  }
  return distances;
}

function assignGraphFocusGroups(cy, focusIds) {
  const focusSet = new Set(focusIds.map((value) => String(value || "").trim()).filter(Boolean));
  const groups = {};
  const queue = [];
  for (const focusId of focusSet) {
    const node = cy.getElementById(focusId);
    if (!node || node.empty() || !node.isNode()) continue;
    groups[focusId] = focusId;
    queue.push(focusId);
  }
  while (queue.length) {
    const currentId = queue.shift();
    const currentNode = cy.getElementById(currentId);
    const groupId = groups[currentId];
    currentNode.connectedEdges().forEach((edge) => {
      edge.connectedNodes().forEach((neighbor) => {
        const neighborId = String(neighbor.id() || "").trim();
        if (!neighborId || Object.prototype.hasOwnProperty.call(groups, neighborId)) return;
        groups[neighborId] = groupId;
        queue.push(neighborId);
      });
    });
  }
  return groups;
}

function polarToPosition(radius, angle) {
  return {
    x: Math.cos(angle) * radius,
    y: Math.sin(angle) * radius,
  };
}

function applyFocusAnchoredPositions(cy, payload, graphElements) {
  const focusIds = graphFocusNodeIds(payload, graphElements);
  if (!focusIds.length) return false;

  const distances = computeGraphFocusDistances(cy, focusIds);
  const groups = assignGraphFocusGroups(cy, focusIds);
  const focusCount = focusIds.length;
  const focusRadius = focusCount > 1 ? Math.max(92, 42 * focusCount) : 0;
  const baseRingRadius = focusCount > 1 ? focusRadius + 148 : 220;
  const ringGap = 156;
  const positions = {};

  focusIds.forEach((focusId, index) => {
    const node = cy.getElementById(focusId);
    if (!node || node.empty() || !node.isNode()) return;
    if (focusCount === 1) {
      positions[focusId] = { x: 0, y: 0 };
      return;
    }
    const angle = (-Math.PI / 2) + ((Math.PI * 2 * index) / focusCount);
    positions[focusId] = polarToPosition(focusRadius, angle);
  });

  const groupedLayers = new Map();
  cy.nodes().forEach((node) => {
    const nodeId = String(node.id() || "").trim();
    if (!nodeId || focusIds.includes(nodeId)) return;
    const distance = Object.prototype.hasOwnProperty.call(distances, nodeId)
      ? Math.max(1, Number(distances[nodeId] || 1))
      : 4;
    const groupId = String(groups[nodeId] || focusIds[0] || "ungrouped");
    const key = `${groupId}::${distance}`;
    if (!groupedLayers.has(key)) groupedLayers.set(key, []);
    groupedLayers.get(key).push(node);
  });

  const focusOrder = new Map(focusIds.map((nodeId, index) => [nodeId, index]));
  const layerEntries = Array.from(groupedLayers.entries()).sort((left, right) => {
    const [leftGroup, leftDistance] = left[0].split("::");
    const [rightGroup, rightDistance] = right[0].split("::");
    return (
      (Number(focusOrder.get(leftGroup) ?? 0) - Number(focusOrder.get(rightGroup) ?? 0))
      || (Number(leftDistance) - Number(rightDistance))
    );
  });

  for (const [key, nodes] of layerEntries) {
    const [groupId, rawDistance] = key.split("::");
    const distance = Math.max(1, Number(rawDistance || 1));
    const groupIndex = Number(focusOrder.get(groupId) ?? 0);
    const centerAngle = (-Math.PI / 2) + ((Math.PI * 2 * groupIndex) / Math.max(1, focusCount));
    const wedge = focusCount > 1 ? Math.min((Math.PI * 1.45) / focusCount, Math.PI / 1.8) : Math.PI * 1.65;
    const radius = baseRingRadius + ((distance - 1) * ringGap);
    const sortedNodes = [...nodes].sort((left, right) => {
      const leftWeight = Number(left.data("support_weight") || 0) + Number(left.data("connected_claim_count") || 0);
      const rightWeight = Number(right.data("support_weight") || 0) + Number(right.data("connected_claim_count") || 0);
      return rightWeight - leftWeight;
    });
    const step = sortedNodes.length > 1 ? wedge / (sortedNodes.length - 1) : 0;
    sortedNodes.forEach((node, index) => {
      const angle = sortedNodes.length === 1
        ? centerAngle
        : centerAngle - (wedge / 2) + (step * index);
      positions[node.id()] = polarToPosition(radius, angle);
    });
  }

  cy.layout({
    name: "preset",
    fit: true,
    padding: 82,
    positions: (node) => positions[node.id()] || { x: 0, y: 0 },
    animate: "end",
    animationDuration: 420,
  }).run();
  return true;
}

function runEvidenceGraphLayout(cy, payload, graphElements) {
  if (!cy) return;
  if (!cy.nodes().length) return;
  if (!cy.edges().length) {
    cy.layout({
      name: "circle",
      animate: "end",
      animationDuration: 280,
      fit: true,
      padding: 78,
      avoidOverlap: true,
      spacingFactor: 1.38,
    }).run();
    return;
  }

  const focusIds = graphFocusNodeIds(payload, graphElements);
  if (!focusIds.length) {
    cy.layout({
      name: "cose",
      animate: "end",
      animationDuration: 380,
      fit: true,
      padding: 76,
      nodeRepulsion: 13600,
      idealEdgeLength: 172,
      edgeElasticity: 96,
      gravity: 0.42,
      numIter: 1200,
    }).run();
    return;
  }
  if (applyFocusAnchoredPositions(cy, payload, graphElements)) return;

  cy.layout({
    name: "cose",
    animate: "end",
    animationDuration: 380,
    fit: true,
    padding: 76,
    nodeRepulsion: 13600,
    idealEdgeLength: 172,
    edgeElasticity: 96,
    gravity: 0.42,
    numIter: 1200,
  }).run();
}

function clearGraphClasses(cy) {
  if (!cy) return;
  cy.elements().removeClass("is-dimmed is-highlighted is-search-match is-selected is-selected-neighbor is-selected-connection");
}

function applyGraphNeighborhoodFocus(cy, focused) {
  if (!cy) return;
  clearGraphClasses(cy);
  if (!focused || focused.empty()) return;
  cy.elements().addClass("is-dimmed");
  focused.removeClass("is-dimmed").addClass("is-highlighted");
  if (typeof focused.neighborhood === "function") {
    focused.neighborhood().removeClass("is-dimmed").addClass("is-highlighted");
  }
}

function applyGraphSelectionHighlight(cy, selection) {
  if (!cy) return;
  cy.elements().removeClass("is-selected is-selected-neighbor is-selected-connection");
  if (!selection || !selection.kind) return;
  if (selection.kind === "node") {
    const nodeId = String(selection?.data?.id || "").trim();
    const node = nodeId ? cy.getElementById(nodeId) : null;
    if (!node || node.empty() || !node.isNode()) return;
    node.addClass("is-selected");
    node.connectedEdges().addClass("is-selected-connection");
    node.connectedEdges().connectedNodes().not(node).addClass("is-selected-neighbor");
    return;
  }
  if (selection.kind === "edge") {
    const edgeId = String(selection?.data?.id || "").trim();
    const edge = edgeId ? cy.getElementById(edgeId) : null;
    if (!edge || edge.empty() || !edge.isEdge()) return;
    edge.addClass("is-selected");
    edge.connectedNodes().addClass("is-selected-neighbor");
  }
}

function applyGraphSearch(taskId, term, { refit = true } = {}) {
  const cy = state.graphCy;
  if (!cy || state.graphRenderedTaskId !== String(taskId || "").trim()) return;
  const normalized = String(term || "").trim().toLowerCase();
  const selection = graphSelectionForTask(taskId);
  state.graphSearchByTaskId[String(taskId || "").trim()] = term;
  cy.elements().removeClass("is-search-match");
  if (!normalized) {
    clearGraphClasses(cy);
    if (selection?.kind === "node") {
      const nodeId = String(selection?.data?.id || "").trim();
      const node = nodeId ? cy.getElementById(nodeId) : null;
      if (node && !node.empty() && node.isNode()) {
        applyGraphNeighborhoodFocus(cy, node.union(node.connectedEdges()).union(node.connectedEdges().connectedNodes()));
      }
    } else if (selection?.kind === "edge") {
      const edgeId = String(selection?.data?.id || "").trim();
      const edge = edgeId ? cy.getElementById(edgeId) : null;
      if (edge && !edge.empty() && edge.isEdge()) {
        applyGraphNeighborhoodFocus(cy, edge.union(edge.connectedNodes()));
      }
    }
    applyGraphSelectionHighlight(cy, selection);
    return;
  }
  const matches = cy.nodes().filter((node) => {
    const data = node.data() || {};
    const aliases = Array.isArray(data.aliases) ? data.aliases.join(" ") : "";
    return [data.full_label, data.label, aliases]
      .map((value) => String(value || "").toLowerCase())
      .some((value) => value.includes(normalized));
  });
  if (matches.empty()) {
    clearGraphClasses(cy);
    applyGraphSelectionHighlight(cy, selection);
    return;
  }
  cy.elements().addClass("is-dimmed");
  matches.removeClass("is-dimmed").addClass("is-search-match");
  matches.connectedEdges().removeClass("is-dimmed").addClass("is-search-match");
  matches.connectedEdges().connectedNodes().removeClass("is-dimmed").addClass("is-search-match");
  applyGraphSelectionHighlight(cy, selection);
  if (refit) {
    cy.animate({
      fit: { eles: matches, padding: 90 },
      duration: 260,
    });
  }
}

function initializeEvidenceGraph(taskId, payload) {
  if (!el.graphCanvas) return;
  if (typeof window.cytoscape !== "function") {
    renderGraphCanvasState("error", "Graph library unavailable", "Cytoscape did not load in the browser.");
    if (el.graphInspector) {
      el.graphInspector.innerHTML = graphSummaryInspectorHtml({
        summary: { node_count: 0, edge_count: 0, evidence_count: 0, mixed_edge_count: 0 },
        warnings: ["Cytoscape failed to load."],
      });
    }
    return;
  }

  const normalizedTaskId = String(taskId || "").trim();
  const graphElements = buildGraphElements(payload);
  if (!graphElements.nodes.length) {
    destroyGraphInstance();
    const emptyState = graphEmptyStateContent(payload);
    renderGraphCanvasState("empty", emptyState.title, emptyState.copy);
    updateGraphInspector(normalizedTaskId);
    return;
  }

  if (state.graphCy) {
    state.graphCy.destroy();
  }

  el.graphCanvas.className = "graph-canvas";
  el.graphCanvas.innerHTML = "";
  state.graphCy = window.cytoscape({
    container: el.graphCanvas,
    elements: [...graphElements.nodes, ...graphElements.edges],
    wheelSensitivity: 0.18,
    style: [
      {
        selector: "node",
        style: {
          "background-color": "data(color)",
          width: "data(size)",
          height: "data(size)",
          label: "data(label)",
          color: "#f3f7fd",
          "font-size": 9.25,
          "font-weight": 600,
          "text-valign": "top",
          "text-halign": "center",
          "text-margin-y": -8,
          "text-wrap": "wrap",
          "text-max-width": 92,
          "text-outline-color": "#0d1117",
          "text-outline-width": 3.1,
          "border-color": "#d8e6ff",
          "border-width": 0.6,
          "overlay-opacity": 0,
        },
      },
      {
        selector: "node[is_focus > 0]",
        style: {
          "border-width": 2.6,
          "border-color": "#ffd98a",
          "shadow-blur": 24,
          "shadow-color": "#ffcf70",
          "shadow-opacity": 0.3,
          "font-size": 10.2,
        },
      },
      {
        selector: "node.is-selected",
        style: {
          width: "mapData(size, 20, 44, 28, 56)",
          height: "mapData(size, 20, 44, 28, 56)",
          "border-width": 4.8,
          "border-color": "#fff5c2",
          "shadow-blur": 42,
          "shadow-color": "#ffe59a",
          "shadow-opacity": 0.64,
          "background-blacken": -0.16,
          "text-outline-width": 4.6,
          "font-size": 10.8,
          "z-index": 9999,
        },
      },
      {
        selector: "node.is-selected-neighbor",
        style: {
          "border-width": 2.2,
          "border-color": "#f0dca4",
          "shadow-blur": 18,
          "shadow-color": "#e4c46f",
          "shadow-opacity": 0.22,
          opacity: 0.88,
        },
      },
      {
        selector: "edge",
        style: {
          width: "data(width)",
          "line-color": "data(color)",
          "target-arrow-color": "data(color)",
          "target-arrow-shape": "triangle",
          "arrow-scale": 0.85,
          "curve-style": "bezier",
          opacity: 0.92,
        },
      },
      {
        selector: "edge.is-selected",
        style: {
          width: "mapData(width, 2, 7, 4.4, 9.4)",
          "line-color": "data(color)",
          "target-arrow-color": "data(color)",
          "arrow-scale": 1.06,
          opacity: 1,
          "z-index": 9999,
        },
      },
      {
        selector: "edge.is-selected-neighbor",
        style: {
          opacity: 0.98,
        },
      },
      {
        selector: "edge.is-selected-connection",
        style: {
          width: "mapData(width, 2, 7, 4.1, 8.8)",
          "line-color": "data(color)",
          "target-arrow-color": "data(color)",
          "arrow-scale": 0.98,
          opacity: 1,
          "z-index": 9998,
        },
      },
      {
        selector: "edge[is_mixed > 0]",
        style: {
          "line-style": "dashed",
        },
      },
      {
        selector: ".is-dimmed",
        style: {
          opacity: 0.08,
        },
      },
      {
        selector: "node.is-dimmed",
        style: {
          "background-blacken": 0.52,
          "border-color": "#465262",
          "text-opacity": 0.24,
          "text-outline-opacity": 0.14,
        },
      },
      {
        selector: "edge.is-dimmed",
        style: {
          opacity: 0.06,
        },
      },
      {
        selector: ".is-highlighted",
        style: {
          opacity: 1,
          "border-width": 2,
          "border-color": "#ffffff",
          "shadow-blur": 22,
          "shadow-color": "#7aa1d8",
          "shadow-opacity": 0.38,
        },
      },
      {
        selector: ".is-search-match",
        style: {
          opacity: 1,
          "border-width": 2,
          "border-color": "#ffe7bc",
          "shadow-blur": 18,
          "shadow-color": "#ffca7a",
          "shadow-opacity": 0.35,
        },
      },
    ],
    layout: {
      name: "preset",
    },
  });

  state.graphRenderedTaskId = normalizedTaskId;
  state.graphRenderedPayload = payload;
  const cy = state.graphCy;
  runEvidenceGraphLayout(cy, payload, graphElements);

  cy.on("tap", "node", (event) => {
    const node = event.target;
    const selection = { kind: "node", data: node.data() };
    setGraphSelection(normalizedTaskId, selection);
    applyGraphNeighborhoodFocus(cy, node.union(node.connectedEdges()).union(node.connectedEdges().connectedNodes()));
    applyGraphSelectionHighlight(cy, selection);
    updateGraphInspector(normalizedTaskId);
  });

  cy.on("tap", "edge", (event) => {
    const edge = event.target;
    const selection = { kind: "edge", data: edge.data() };
    setGraphSelection(normalizedTaskId, selection);
    applyGraphNeighborhoodFocus(cy, edge.union(edge.connectedNodes()));
    applyGraphSelectionHighlight(cy, selection);
    updateGraphInspector(normalizedTaskId);
  });

  cy.on("mouseover", "node, edge", (event) => {
    if (el.graphCanvas) el.graphCanvas.style.cursor = "pointer";
    if (graphSelectionForTask(normalizedTaskId)) return;
    const target = event.target;
    const focus = target.isNode && target.isNode()
      ? target.union(target.connectedEdges()).union(target.connectedEdges().connectedNodes())
      : target.union(target.connectedNodes());
    applyGraphNeighborhoodFocus(cy, focus);
  });

  cy.on("mouseout", "node, edge", () => {
    if (el.graphCanvas) el.graphCanvas.style.cursor = "";
    if (graphSelectionForTask(normalizedTaskId)) return;
    const term = String(state.graphSearchByTaskId[normalizedTaskId] || "").trim();
    if (term) {
      applyGraphSearch(normalizedTaskId, term, { refit: false });
      return;
    }
    clearGraphClasses(cy);
  });

  cy.on("tap", (event) => {
    if (event.target !== cy) return;
    setGraphSelection(normalizedTaskId, null);
    const term = String(state.graphSearchByTaskId[normalizedTaskId] || "").trim();
    if (term) applyGraphSearch(normalizedTaskId, term, { refit: false });
    else clearGraphClasses(cy);
    applyGraphSelectionHighlight(cy, null);
    updateGraphInspector(normalizedTaskId);
  });

  const existingTerm = String(state.graphSearchByTaskId[normalizedTaskId] || "").trim();
  if (existingTerm) applyGraphSearch(normalizedTaskId, existingTerm, { refit: false });
  applyGraphSelectionHighlight(cy, graphSelectionForTask(normalizedTaskId));
  updateGraphInspector(normalizedTaskId);
}

function debugSummaryHtml(debugPayload) {
  const payload = debugPayload && typeof debugPayload === "object" ? debugPayload : {};
  const rawState = payload.state && typeof payload.state === "object" ? payload.state : {};
  const workflow = rawState.workflow_task_state && typeof rawState.workflow_task_state === "object"
    ? rawState.workflow_task_state
    : {};
  const evidenceStore = workflow.evidence_store && typeof workflow.evidence_store === "object"
    ? workflow.evidence_store
    : {};
  const executionMetrics = workflow.execution_metrics && typeof workflow.execution_metrics === "object"
    ? workflow.execution_metrics
    : {};
  const executionSummary = executionMetrics.summary && typeof executionMetrics.summary === "object"
    ? executionMetrics.summary
    : {};
  const steps = Array.isArray(workflow.steps) ? workflow.steps : [];
  const completedSteps = steps.filter((step) => String(step?.status || "").trim() === "completed").length;
  const priorResearch = Array.isArray(rawState.co_scientist_prior_research) ? rawState.co_scientist_prior_research : [];
  const watchlist = Array.isArray(executionSummary.specialization_watchlist)
    ? executionSummary.specialization_watchlist
    : [];
  const metrics = [
    { label: "Source", value: String(payload.source || "none") || "none" },
    { label: "Plan", value: String(workflow.plan_status || "unknown") || "unknown" },
    { label: "Steps", value: steps.length ? `${completedSteps}/${steps.length}` : "0" },
    {
      label: "Evidence",
      value: `${countCollectionItems(evidenceStore.entities)} entities / ${countCollectionItems(evidenceStore.claims)} claims / ${countCollectionItems(evidenceStore.evidence)} evidence`,
    },
    {
      label: "Exec metrics",
      value: String(executionSummary.total_steps || steps.length || 0),
    },
    { label: "Prior research", value: String(priorResearch.length) },
    { label: "Approval pending", value: rawState.co_scientist_plan_pending_approval ? "yes" : "no" },
    {
      label: "Persisted",
      value: payload.persisted_updated_at ? formatDate(payload.persisted_updated_at) : "n/a",
    },
  ];

  const metricHtml = metrics
    .map((item) => `
      <div class="debug-metric">
        <span class="debug-metric-label">${escapeHtml(item.label)}</span>
        <span class="debug-metric-value">${escapeHtml(item.value)}</span>
      </div>
    `)
    .join("");

  const watchlistHtml = watchlist.length
    ? `
      <div class="debug-watchlist">
        ${watchlist.slice(0, 6).map((item) => `<span class="debug-pill">${escapeHtml(String(item || "").trim())}</span>`).join("")}
      </div>
    `
    : "";

  return `
    <div class="debug-metrics-grid">${metricHtml}</div>
    ${watchlistHtml}
  `;
}

async function refreshCurrentDebugState({ force = false } = {}) {
  const taskId = currentDebugTaskId();
  if (!taskId || !state.debugOpen) return;
  const existing = state.debugByTaskId[taskId];
  if (existing?.loading) return;
  if (!force && existing && existing.data) return;

  const hasExistingData = Boolean(existing?.data);
  state.debugByTaskId[taskId] = hasExistingData
    ? { loading: true, error: "", data: existing.data }
    : { loading: true, error: "", data: null };
  if (!hasExistingData) renderReportPanel();

  try {
    const payload = await api(`/api/tasks/${encodeURIComponent(taskId)}/debug/workflow-state`);
    state.debugByTaskId[taskId] = { loading: false, error: "", data: payload };
  } catch (err) {
    state.debugByTaskId[taskId] = hasExistingData
      ? { loading: false, error: "", data: existing.data }
      : {
          loading: false,
          error: String(err?.message || "Failed to load debug state."),
          data: null,
        };
  }

  renderReportPanel();
}

async function refreshCurrentEvidenceGraph({ force = false } = {}) {
  const taskId = currentGraphTaskId();
  if (!taskId || state.reportPanelMode !== "graph") return;
  const existing = state.graphByTaskId[taskId];
  if (existing?.loading) return;
  if (!force && existing && existing.data) return;

  const hasExistingData = Boolean(existing?.data);
  state.graphByTaskId[taskId] = hasExistingData
    ? { loading: true, error: "", data: existing.data }
    : { loading: true, error: "", data: null };
  if (!hasExistingData) renderReportPanel();

  try {
    const payload = await api(`/api/tasks/${encodeURIComponent(taskId)}/evidence-graph`);
    state.graphByTaskId[taskId] = { loading: false, error: "", data: payload };
  } catch (err) {
    state.graphByTaskId[taskId] = hasExistingData
      ? { loading: false, error: "", data: existing.data }
      : {
          loading: false,
          error: String(err?.message || "Failed to load evidence graph."),
          data: null,
        };
  }

  renderReportPanel();
}

function visualsDimensionMap(payload) {
  const dimensions = Array.isArray(payload?.visualizations?.dimensions)
    ? payload.visualizations.dimensions
    : [];
  return new Map(dimensions.map((dimension) => [String(dimension?.key || ""), dimension]));
}

function visualsScoreColor(value, maxScore = 5) {
  const numeric = Number(value);
  const ratio = Number.isFinite(numeric) ? Math.max(0, Math.min(1, numeric / maxScore)) : 0;
  const hue = 10 + ratio * 122;
  const lightness = 18 + ratio * 22;
  return `hsla(${hue}, 78%, ${lightness}%, 0.92)`;
}

function visualsMetricLabel(key) {
  return String(key || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function visualsMetricValue(value) {
  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : value.toFixed(2);
  }
  if (typeof value === "boolean") return value ? "Yes" : "No";
  return String(value || "");
}

function visualsPanelStateHtml(title, message, kind = "empty") {
  return `
    <div class="visuals-empty visuals-empty-${escapeHtml(kind)}">
      <h4>${escapeHtml(title)}</h4>
      <p>${escapeHtml(message)}</p>
    </div>
  `;
}

function visualsBarChartSvg(rows, maxScore = 5) {
  const safeRows = Array.isArray(rows) ? rows.slice(0, 8) : [];
  const width = 760;
  const left = 170;
  const right = 56;
  const top = 18;
  const rowHeight = 42;
  const chartHeight = top + safeRows.length * rowHeight + 26;
  const innerWidth = width - left - right;
  const ticks = [0, 1, 2, 3, 4, 5];
  const tickLines = ticks.map((tick) => {
    const x = left + (tick / maxScore) * innerWidth;
    return `
      <line x1="${x}" y1="${top - 6}" x2="${x}" y2="${chartHeight - 18}" class="visual-axis-line" />
      <text x="${x}" y="${chartHeight - 2}" text-anchor="middle" class="visual-axis-text">${tick}</text>
    `;
  }).join("");
  const bars = safeRows.map((row, index) => {
    const score = Number(row?.overall_score || 0);
    const y = top + index * rowHeight;
    const barWidth = Math.max(0, Math.min(innerWidth, (score / maxScore) * innerWidth));
    return `
      <text x="${left - 10}" y="${y + 17}" text-anchor="end" class="visual-label-text">${escapeHtml(row?.short_label || row?.dataset_label || `Dataset ${index + 1}`)}</text>
      <rect x="${left}" y="${y}" width="${innerWidth}" height="22" rx="10" class="visual-bar-track" />
      <rect x="${left}" y="${y}" width="${barWidth}" height="22" rx="10" fill="${visualsScoreColor(score, maxScore)}" />
      <text x="${left + barWidth + 8}" y="${y + 16}" class="visual-value-text">${escapeHtml(score.toFixed(2))}</text>
    `;
  }).join("");
  return `
    <svg viewBox="0 0 ${width} ${chartHeight}" class="visual-svg" role="img" aria-label="Overall dataset ranking">
      ${tickLines}
      ${bars}
    </svg>
  `;
}

function visualsScatterPlotSvg(payload) {
  const bundle = payload?.visualizations || {};
  const rows = Array.isArray(bundle.rows) ? bundle.rows.slice(0, 10) : [];
  const scatter = bundle?.charts?.scatter || {};
  const dimensions = visualsDimensionMap(payload);
  const xKey = String(scatter?.x_dimension || "");
  const yKey = String(scatter?.y_dimension || "");
  const sizeKey = String(scatter?.size_key || "overall_score");
  const sizeKind = String(scatter?.size_kind || "overall");
  if (!xKey || !yKey || rows.length < 2) {
    return visualsPanelStateHtml("Scatter unavailable", "Need at least two datasets and two scored dimensions to plot the comparison landscape.");
  }

  const width = 760;
  const height = 360;
  const left = 66;
  const right = 34;
  const top = 18;
  const bottom = 48;
  const innerWidth = width - left - right;
  const innerHeight = height - top - bottom;
  const ticks = [0, 1, 2, 3, 4, 5];
  const sizeValues = rows.map((row) => {
    if (sizeKind === "metric") return Number(row?.metrics?.[sizeKey] || 0);
    if (sizeKind === "dimension") return Number(row?.scores?.[sizeKey] || 0);
    return Number(row?.overall_score || 0);
  });
  const minSize = Math.min(...sizeValues);
  const maxSize = Math.max(...sizeValues);
  const normalizeRadius = (value) => {
    if (!Number.isFinite(value)) return 9;
    if (Math.abs(maxSize - minSize) < 1e-6) return 11;
    return 7 + ((value - minSize) / (maxSize - minSize)) * 13;
  };
  const grid = ticks.map((tick) => {
    const x = left + (tick / 5) * innerWidth;
    const y = top + innerHeight - (tick / 5) * innerHeight;
    return `
      <line x1="${x}" y1="${top}" x2="${x}" y2="${top + innerHeight}" class="visual-axis-line" />
      <line x1="${left}" y1="${y}" x2="${left + innerWidth}" y2="${y}" class="visual-axis-line" />
      <text x="${x}" y="${height - 12}" text-anchor="middle" class="visual-axis-text">${tick}</text>
      <text x="${left - 12}" y="${y + 4}" text-anchor="end" class="visual-axis-text">${tick}</text>
    `;
  }).join("");
  const points = rows.map((row, index) => {
    const xScore = Number(row?.scores?.[xKey] || 0);
    const yScore = Number(row?.scores?.[yKey] || 0);
    const sizeValue = sizeValues[index];
    const cx = left + (xScore / 5) * innerWidth;
    const cy = top + innerHeight - (yScore / 5) * innerHeight;
    const r = normalizeRadius(sizeValue);
    return `
      <circle cx="${cx}" cy="${cy}" r="${r}" fill="${visualsScoreColor(row?.overall_score || 0, 5)}" class="visual-point" />
      <text x="${cx}" y="${cy - r - 6}" text-anchor="middle" class="visual-point-label">${escapeHtml(row?.short_label || row?.dataset_label || `Dataset ${index + 1}`)}</text>
    `;
  }).join("");
  const xLabel = dimensions.get(xKey)?.label || visualsMetricLabel(xKey);
  const yLabel = dimensions.get(yKey)?.label || visualsMetricLabel(yKey);
  const sizeLabel = sizeKind === "metric"
    ? visualsMetricLabel(sizeKey)
    : (dimensions.get(sizeKey)?.label || visualsMetricLabel(sizeKey));
  return `
    <div class="visual-scatter-meta">Bubble size: ${escapeHtml(sizeLabel)}</div>
    <svg viewBox="0 0 ${width} ${height}" class="visual-svg" role="img" aria-label="Dataset comparison scatter plot">
      ${grid}
      <line x1="${left}" y1="${top + innerHeight}" x2="${left + innerWidth}" y2="${top + innerHeight}" class="visual-axis-strong" />
      <line x1="${left}" y1="${top}" x2="${left}" y2="${top + innerHeight}" class="visual-axis-strong" />
      ${points}
      <text x="${left + innerWidth / 2}" y="${height - 2}" text-anchor="middle" class="visual-axis-label">${escapeHtml(xLabel)}</text>
      <text x="18" y="${top + innerHeight / 2}" transform="rotate(-90 18 ${top + innerHeight / 2})" text-anchor="middle" class="visual-axis-label">${escapeHtml(yLabel)}</text>
    </svg>
  `;
}

function visualsHeatmapHtml(payload) {
  const bundle = payload?.visualizations || {};
  const rows = Array.isArray(bundle.rows) ? bundle.rows.slice(0, 10) : [];
  const dimensions = Array.isArray(bundle.dimensions) ? bundle.dimensions : [];
  if (!rows.length || !dimensions.length) {
    return visualsPanelStateHtml("Heatmap unavailable", "No comparison matrix is available yet.");
  }
  const head = dimensions.map((dimension) => `<th>${escapeHtml(String(dimension?.label || ""))}</th>`).join("");
  const body = rows.map((row) => `
    <tr>
      <th>${escapeHtml(row?.dataset_label || "")}</th>
      ${dimensions.map((dimension) => {
        const score = Number(row?.scores?.[dimension?.key] || 0);
        return `<td><span class="visual-heat-cell" style="background:${visualsScoreColor(score, 5)}">${escapeHtml(score.toFixed(1))}</span></td>`;
      }).join("")}
    </tr>
  `).join("");
  return `
    <div class="visual-heatmap-wrap">
      <table class="visual-heatmap-table">
        <thead>
          <tr>
            <th>Dataset</th>
            ${head}
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function datasetVisualizationsHtml(payload) {
  const bundle = payload?.visualizations || {};
  const rows = Array.isArray(bundle.rows) ? bundle.rows : [];
  if (!payload?.available || !rows.length) {
    return visualsPanelStateHtml("No visual summary yet", payload?.message || "No analysis visuals are available for this task yet.");
  }

  const summaryCards = bundle.summary_cards || {};
  const notes = Array.isArray(bundle.notes) ? bundle.notes : [];
  const warnings = Array.isArray(bundle.warnings) ? bundle.warnings : [];
  const metricCards = [
    { label: "Datasets", value: String(summaryCards.dataset_count || rows.length) },
    { label: "Dimensions", value: String(summaryCards.dimension_count || 0) },
    { label: "Sources", value: String(summaryCards.source_count || 0) },
    { label: "Top Candidate", value: String(summaryCards.top_dataset || rows[0]?.dataset_label || "") },
  ].map((item) => `
    <div class="visual-stat-card">
      <span class="visual-stat-label">${escapeHtml(item.label)}</span>
      <span class="visual-stat-value">${escapeHtml(item.value)}</span>
    </div>
  `).join("");

  const datasetCards = rows.slice(0, 8).map((row) => {
    const metrics = row?.metrics && typeof row.metrics === "object"
      ? Object.entries(row.metrics).slice(0, 4)
      : [];
    const tags = Array.isArray(row?.tags) ? row.tags : [];
    return `
      <article class="visual-dataset-card">
        <div class="visual-dataset-head">
          <div>
            <h5>${escapeHtml(row?.dataset_label || "")}</h5>
            <p>${escapeHtml([row?.source, row?.accession].filter(Boolean).join(" • "))}</p>
          </div>
          <span class="visual-score-pill">${escapeHtml(Number(row?.overall_score || 0).toFixed(2))}/5</span>
        </div>
        ${row?.notes ? `<p class="visual-dataset-notes">${escapeHtml(row.notes)}</p>` : ""}
        ${tags.length ? `<div class="visual-tag-row">${tags.map((tag) => `<span class="visual-tag">${escapeHtml(String(tag || ""))}</span>`).join("")}</div>` : ""}
        ${metrics.length ? `<div class="visual-metric-row">${metrics.map(([key, value]) => `<span class="visual-metric-chip"><strong>${escapeHtml(visualsMetricLabel(key))}:</strong> ${escapeHtml(visualsMetricValue(value))}</span>`).join("")}</div>` : ""}
      </article>
    `;
  }).join("");

  return `
    <div class="visuals-shell">
      <section class="visuals-intro">
        <div>
          <h4>${escapeHtml(bundle.summary || bundle.objective || "Dataset comparison")}</h4>
          <p>${escapeHtml(bundle.objective || "Analysis-mode visual summary generated from the current dataset comparison.")}</p>
        </div>
        <div class="visual-stat-grid">${metricCards}</div>
      </section>
      <section class="visual-chart-card">
        <div class="visual-card-head">
          <h5>Overall ranking</h5>
          <p>Weighted comparison across the selected reuse-readiness dimensions.</p>
        </div>
        ${visualsBarChartSvg(rows, Number(bundle?.charts?.bar?.max_score || 5))}
      </section>
      <div class="visual-grid-two">
        <section class="visual-chart-card">
          <div class="visual-card-head">
            <h5>Dimension heatmap</h5>
            <p>How each dataset scores on each comparison dimension.</p>
          </div>
          ${visualsHeatmapHtml(payload)}
        </section>
        <section class="visual-chart-card">
          <div class="visual-card-head">
            <h5>Comparison landscape</h5>
            <p>Higher-right candidates perform better across the plotted dimensions.</p>
          </div>
          ${visualsScatterPlotSvg(payload)}
        </section>
      </div>
      <section class="visual-chart-card">
        <div class="visual-card-head">
          <h5>Dataset notes</h5>
          <p>Compact rationale and metadata for the strongest candidates in the current shortlist.</p>
        </div>
        <div class="visual-dataset-grid">${datasetCards}</div>
      </section>
      ${notes.length ? `<section class="visual-text-block"><h5>Analysis Notes</h5><ul>${notes.map((note) => `<li>${escapeHtml(String(note || ""))}</li>`).join("")}</ul></section>` : ""}
      ${warnings.length ? `<section class="visual-text-block warning"><h5>Warnings</h5><ul>${warnings.map((warning) => `<li>${escapeHtml(String(warning || ""))}</li>`).join("")}</ul></section>` : ""}
    </div>
  `;
}

function analysisWorkspaceStateHtml(title, message, kind = "empty") {
  return `
    <div class="analysis-empty analysis-empty-${escapeHtml(kind)}">
      <h4>${escapeHtml(title)}</h4>
      <p>${escapeHtml(message)}</p>
    </div>
  `;
}

function analysisNotebookValueHtml(value) {
  if (Array.isArray(value)) {
    const items = value.map((item) => escapeHtml(String(item || ""))).filter(Boolean);
    if (!items.length) return '<span class="analysis-value-muted">Not provided</span>';
    return items.map((item) => `<span class="analysis-chip">${item}</span>`).join("");
  }
  const text = String(value ?? "").trim();
  return text ? escapeHtml(text) : '<span class="analysis-value-muted">Not provided</span>';
}

function analysisMetadataSectionHtml(title, payload, { allowChips = false } = {}) {
  const entries = Object.entries(payload && typeof payload === "object" ? payload : {})
    .filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      return String(value ?? "").trim().length > 0;
    });
  if (!entries.length) return "";
  return `
    <section class="analysis-metadata-section">
      <div class="analysis-metadata-title">${escapeHtml(title)}</div>
      <div class="analysis-metadata-table">
        ${entries.map(([key, value]) => `
          <div class="analysis-metadata-row">
            <div class="analysis-metadata-key">${escapeHtml(key.replace(/_/g, " "))}</div>
            <div class="analysis-metadata-value ${allowChips && Array.isArray(value) ? "analysis-metadata-value-chips" : ""}">${analysisNotebookValueHtml(value)}</div>
          </div>
        `).join("")}
      </div>
    </section>
  `;
}

function analysisComparisonDatasetListHtml(datasets = []) {
  const rows = Array.isArray(datasets) ? datasets : [];
  if (!rows.length) return "";
  return `
    <section class="analysis-comparison-list">
      <div class="analysis-cell-subtitle">Shortlist</div>
      <div class="analysis-table-shell">
        <table class="analysis-table">
          <thead>
            <tr>
              <th>Dataset</th>
              <th>Accession</th>
              <th>Subjects</th>
              <th>Score</th>
              <th>Notes</th>
            </tr>
          </thead>
          <tbody>
            ${rows.slice(0, 12).map((dataset) => {
              const notes = String(dataset?.notes || "").trim();
              const tags = Array.isArray(dataset?.tags) ? dataset.tags : [];
              const subjectCount = dataset?.metrics?.subject_count;
              return `
                <tr>
                  <td>
                    <div class="analysis-table-title">${escapeHtml(dataset?.dataset_label || "")}</div>
                    ${tags.length ? `<div class="analysis-table-tags">${tags.map((tag) => `<span class="analysis-chip">${escapeHtml(String(tag || ""))}</span>`).join("")}</div>` : ""}
                  </td>
                  <td>${escapeHtml([dataset?.source, dataset?.accession].filter(Boolean).join(" • "))}</td>
                  <td>${subjectCount != null && subjectCount !== "" ? escapeHtml(String(subjectCount)) : '<span class="analysis-value-muted">n/a</span>'}</td>
                  <td>${dataset?.overall_score != null ? `<span class="visual-score-pill">${escapeHtml(Number(dataset.overall_score || 0).toFixed(2))}/5</span>` : '<span class="analysis-value-muted">n/a</span>'}</td>
                  <td>${notes ? escapeHtml(notes) : '<span class="analysis-value-muted">No note</span>'}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function analysisIterationForTask(taskId) {
  const detail = state.selectedConversationDetail;
  if (!detail) return null;
  return findIteration(detail, taskId);
}

function analysisNotebookPromptHtml(taskId) {
  const iteration = analysisIterationForTask(taskId);
  const task = iteration?.task || {};
  const prompt = String(task?.user_query || task?.objective || "").trim();
  if (!prompt) return "";
  return `
    <section class="analysis-notebook-block is-input">
      <div class="analysis-notebook-block-label">In</div>
      <pre class="analysis-notebook-input">${escapeHtml(prompt)}</pre>
    </section>
  `;
}

function analysisNotebookExecutionHtml(taskId) {
  const iteration = analysisIterationForTask(taskId);
  const steps = Array.isArray(iteration?.task?.steps) ? iteration.task.steps : [];
  const events = Array.isArray(iteration?.research_log?.events) ? iteration.research_log.events : [];
  const visibleSteps = steps.filter((step) => String(step?.status || "pending").trim() !== "pending");
  const hasNotebookActivity = notebookActivityEventLines(events).length > 0;
  if (!visibleSteps.length && !hasNotebookActivity) return "";
  return `
    <section class="analysis-notebook-block is-trace">
      <div class="analysis-notebook-block-label">Trace</div>
      <p class="analysis-notebook-hint">Agent execution trace for this cell.</p>
      ${buildActivityDetailsHtml(visibleSteps, events)}
    </section>
  `;
}

function analysisMetadataMetricNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const text = String(value || "").trim();
  if (!text) return null;
  const match = text.replace(/,/g, "").match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function analysisMetadataVisualHtml(payload) {
  const metricSpecs = [
    {
      label: "Subjects",
      raw: payload?.subjects_sessions?.subject_count,
    },
    {
      label: "Sessions",
      raw: payload?.subjects_sessions?.session_count,
    },
    {
      label: "Assets",
      raw: payload?.access_download?.asset_count,
    },
  ];
  const metrics = metricSpecs
    .map((item) => {
      const value = analysisMetadataMetricNumber(item.raw);
      if (value == null) return null;
      return {
        label: item.label,
        value,
        display: Number.isInteger(value) ? String(value) : value.toFixed(1),
      };
    })
    .filter(Boolean);

  const chips = [];
  const publicDownload = String(payload?.access_download?.public_download || "").trim();
  const license = String(payload?.access_download?.license || "").trim();
  const source = String(payload?.source || "").trim();
  if (source) chips.push(source);
  if (publicDownload) chips.push(`Public download: ${publicDownload}`);
  if (license) chips.push(`License: ${license}`);

  if (!metrics.length && !chips.length) return "";
  const maxValue = Math.max(...metrics.map((metric) => Number(metric.value || 0)), 1);

  return `
    <section class="analysis-notebook-block is-output">
      <div class="analysis-notebook-block-label">Visual summary</div>
      ${chips.length ? `<div class="analysis-tag-row analysis-metadata-hero-tags">${chips.map((chip) => `<span class="analysis-chip">${escapeHtml(chip)}</span>`).join("")}</div>` : ""}
      ${metrics.length ? `
        <div class="analysis-metric-grid">
          ${metrics.map((metric) => `
            <div class="analysis-metric-card">
              <div class="analysis-metric-head">
                <span>${escapeHtml(metric.label)}</span>
                <strong>${escapeHtml(metric.display)}</strong>
              </div>
              <div class="analysis-metric-bar">
                <span style="width:${Math.max(16, Math.round((Number(metric.value || 0) / maxValue) * 100))}%"></span>
              </div>
            </div>
          `).join("")}
        </div>
      ` : ""}
    </section>
  `;
}

function analysisNotebookDatasetComparisonCodeHtml(payload) {
  const datasets = Array.isArray(payload?.datasets) ? payload.datasets : [];
  const dims = Array.isArray(payload?.dimensions) ? payload.dimensions : [];
  if (!datasets.length) return "";
  const firstAccession = String(datasets[0]?.accession || "dsXXXXXX").trim();
  const dimKeys = dims
    .map((d) => String(d?.key || d?.label || "").trim())
    .filter(Boolean);
  const lines = [
    "query = \"schizophrenia\"",
    "datasets = search_openneuro_datasets(query=query)",
    "records = [get_openneuro_dataset(ds_id) for ds_id in datasets]",
    "",
    "# normalized comparison axes",
    `dimensions = ${JSON.stringify(dimKeys.length ? dimKeys : ["sample_scale", "metadata_richness", "access_readiness"])}`,
    "",
    "# preview",
    `records[:${Math.min(datasets.length, 6)}]  # includes ${firstAccession}`,
    "",
    "# output below is rendered from stored notebook artifacts",
  ];
  return `
    <section class="analysis-notebook-block is-input">
      <div class="analysis-notebook-block-label">In</div>
      <pre class="analysis-notebook-input">${escapeHtml(lines.join("\n"))}</pre>
    </section>
  `;
}

function analysisNotebookComparisonOutputHtml(payload) {
  const bundle = payload?.visualizations || {};
  const rows = Array.isArray(bundle?.rows) ? bundle.rows : [];
  const notes = Array.isArray(bundle?.notes) ? bundle.notes : [];
  const warnings = Array.isArray(bundle?.warnings) ? bundle.warnings : [];
  if (!rows.length) {
    return `
      <section class="analysis-notebook-block is-output">
        <div class="analysis-notebook-block-label">Out</div>
        ${analysisComparisonDatasetListHtml(payload?.datasets || [])}
      </section>
    `;
  }
  return `
    <section class="analysis-notebook-block is-output">
      <div class="analysis-notebook-block-label">Out</div>
      <div class="analysis-notebook-output-stack">
        ${analysisComparisonDatasetListHtml(payload?.datasets || [])}
        <section class="analysis-notebook-figure">
          <div class="analysis-cell-subtitle">Overall ranking</div>
          ${visualsBarChartSvg(rows, Number(bundle?.charts?.bar?.max_score || 5))}
        </section>
        <div class="analysis-notebook-chart-grid">
          <section class="analysis-notebook-figure">
            <div class="analysis-cell-subtitle">Heatmap</div>
            ${visualsHeatmapHtml({ available: true, visualizations: bundle })}
          </section>
          <section class="analysis-notebook-figure">
            <div class="analysis-cell-subtitle">Scatter</div>
            ${visualsScatterPlotSvg({ available: true, visualizations: bundle })}
          </section>
        </div>
        ${notes.length ? `<section class="analysis-notebook-figure"><div class="analysis-cell-subtitle">Notes</div><ul class="analysis-inline-list">${notes.map((note) => `<li>${escapeHtml(String(note || ""))}</li>`).join("")}</ul></section>` : ""}
        ${warnings.length ? `<section class="analysis-notebook-figure is-warning"><div class="analysis-cell-subtitle">Warnings</div><ul class="analysis-inline-list">${warnings.map((warning) => `<li>${escapeHtml(String(warning || ""))}</li>`).join("")}</ul></section>` : ""}
      </div>
    </section>
  `;
}

function analysisNotebookCellHtml(cell, artifact, workspace) {
  const type = String(cell?.type || "").trim();
  const taskId = String(cell?.task_id || "").trim();
  const isFocused = taskId && taskId === String(state.selectedReportTaskId || "").trim();
  const title = String(cell?.title || artifact?.title || "Notebook cell");
  if (type === "dataset_comparison") {
    const payload = artifact?.payload || {};
    return `
      <article class="analysis-cell ${isFocused ? "is-focused" : ""}" data-cell-id="${escapeHtml(cell?.cell_id || "")}" data-task-id="${escapeHtml(taskId)}">
        <header class="analysis-cell-head">
          <div>
            <div class="analysis-cell-kicker">Dataset comparison</div>
            <h4>${escapeHtml(title)}</h4>
          </div>
          <span class="analysis-cell-date">${escapeHtml(formatDate(cell?.created_at))}</span>
        </header>
        ${analysisNotebookPromptHtml(taskId)}
        ${analysisNotebookExecutionHtml(taskId)}
        ${analysisNotebookDatasetComparisonCodeHtml(payload)}
        ${analysisNotebookComparisonOutputHtml(payload)}
      </article>
    `;
  }
  if (type === "dataset_metadata") {
    const payload = artifact?.payload || {};
    return `
      <article class="analysis-cell ${isFocused ? "is-focused" : ""}" data-cell-id="${escapeHtml(cell?.cell_id || "")}" data-task-id="${escapeHtml(taskId)}">
        <header class="analysis-cell-head">
          <div>
            <div class="analysis-cell-kicker">Dataset metadata</div>
            <h4>${escapeHtml(title)}</h4>
            <p class="analysis-cell-caption">${escapeHtml(String(payload?.summary || payload?.dataset_label || ""))}</p>
          </div>
          <span class="analysis-cell-date">${escapeHtml(formatDate(cell?.created_at))}</span>
        </header>
        ${analysisNotebookPromptHtml(taskId)}
        ${analysisNotebookExecutionHtml(taskId)}
        ${analysisMetadataVisualHtml(payload)}
        <section class="analysis-notebook-block is-output">
          <div class="analysis-notebook-block-label">Out</div>
          <div class="analysis-metadata-grid">
            ${analysisMetadataSectionHtml("Identity", payload?.identity || {})}
            ${analysisMetadataSectionHtml("Access / Download", payload?.access_download || {})}
            ${analysisMetadataSectionHtml("Subjects / Sessions", payload?.subjects_sessions || {})}
            ${analysisMetadataSectionHtml("Modalities / Tasks", payload?.modalities_tasks || {}, { allowChips: true })}
            ${analysisMetadataSectionHtml("Standards / File Formats", payload?.standards_file_formats || {}, { allowChips: true })}
            ${analysisMetadataSectionHtml("Links / Publications", payload?.links_publications || {}, { allowChips: true })}
          </div>
          ${Array.isArray(payload?.source_raw_highlights) && payload.source_raw_highlights.length ? `
            <section class="analysis-metadata-section">
              <div class="analysis-metadata-title">Source-native raw highlights</div>
              <div class="analysis-tag-row">
                ${payload.source_raw_highlights.map((item) => `<span class="analysis-chip">${escapeHtml(String(item || ""))}</span>`).join("")}
              </div>
            </section>
          ` : ""}
        </section>
      </article>
    `;
  }
  if (type === "analysis_note") {
    const markdown = String(artifact?.payload?.markdown || "").trim();
    return `
      <article class="analysis-cell ${isFocused ? "is-focused" : ""}" data-cell-id="${escapeHtml(cell?.cell_id || "")}" data-task-id="${escapeHtml(taskId)}">
        <header class="analysis-cell-head">
          <div>
            <div class="analysis-cell-kicker">Analysis note</div>
            <h4>${escapeHtml(title)}</h4>
          </div>
          <span class="analysis-cell-date">${escapeHtml(formatDate(cell?.created_at))}</span>
        </header>
        ${analysisNotebookPromptHtml(taskId)}
        ${analysisNotebookExecutionHtml(taskId)}
        <section class="analysis-notebook-block is-output">
          <div class="analysis-notebook-block-label">Out</div>
          <div class="analysis-note markdown-body">${markdownToHtml(markdown)}</div>
        </section>
      </article>
    `;
  }
  return `
    <article class="analysis-cell" data-cell-id="${escapeHtml(cell?.cell_id || "")}" data-task-id="${escapeHtml(taskId)}">
      <header class="analysis-cell-head">
        <h4>${escapeHtml(title)}</h4>
      </header>
      <div class="analysis-note markdown-body">${markdownToHtml(String(artifact?.summary || "").trim())}</div>
    </article>
  `;
}

function analysisNotebookMimeData(data, key) {
  if (!data || typeof data !== "object") return "";
  const value = data[key];
  if (Array.isArray(value)) return value.join("");
  return typeof value === "string" ? value : "";
}

function analysisNotebookOutputHtml(output) {
  const outputType = String(output?.output_type || "").trim();
  if (outputType === "display_data" || outputType === "execute_result") {
    const data = output?.data && typeof output.data === "object" ? output.data : {};
    const html = analysisNotebookMimeData(data, "text/html");
    const svg = analysisNotebookMimeData(data, "image/svg+xml");
    const png = analysisNotebookMimeData(data, "image/png");
    const plain = analysisNotebookMimeData(data, "text/plain");
    const jsonValue = data["application/json"];
    return `
      <div class="nb-output-item">
        ${html ? `<div class="nb-output-rich">${html}</div>` : ""}
        ${svg ? `<div class="nb-output-svg">${svg}</div>` : ""}
        ${png ? `<div class="nb-output-image"><img src="data:image/png;base64,${png}" alt="${escapeHtml(plain || "Notebook figure")}" /></div>` : ""}
        ${plain && !html && !svg && !png ? `<pre class="nb-output-plain">${escapeHtml(plain)}</pre>` : ""}
        ${jsonValue && !html ? `<pre class="nb-output-json">${escapeHtml(JSON.stringify(jsonValue, null, 2))}</pre>` : ""}
      </div>
    `;
  }
  if (outputType === "stream") {
    const text = Array.isArray(output?.text) ? output.text.join("") : String(output?.text || "");
    return `<div class="nb-output-item"><pre class="nb-output-plain">${escapeHtml(text)}</pre></div>`;
  }
  if (outputType === "error") {
    const traceback = Array.isArray(output?.traceback) ? output.traceback.join("\n") : String(output?.evalue || "Notebook error");
    return `<div class="nb-output-item is-error"><pre class="nb-output-plain">${escapeHtml(traceback)}</pre></div>`;
  }
  return "";
}

function analysisNotebookRenderedCellHtml(cell, index) {
  const cellType = String(cell?.cell_type || "").trim();
  const meta = cell?.metadata?.co_scientist || {};
  const taskId = String(meta?.task_id || "").trim();
  const title = String(meta?.title || "").trim();
  const createdAt = String(meta?.created_at || "").trim();
  const kind = String(meta?.kind || "").trim();
  if (cellType === "markdown") {
    const source = Array.isArray(cell?.source) ? cell.source.join("") : String(cell?.source || "");
    return `
      <article class="nb-cell nb-cell-markdown" data-task-id="${escapeHtml(taskId)}" data-cell-index="${index}">
        ${title ? `<div class="nb-cell-title">${escapeHtml(title)}</div>` : ""}
        <div class="nb-markdown markdown-body">${markdownToHtml(source)}</div>
      </article>
    `;
  }
  const source = Array.isArray(cell?.source) ? cell.source.join("") : String(cell?.source || "");
  const outputs = Array.isArray(cell?.outputs) ? cell.outputs : [];
  return `
    <article class="nb-cell nb-cell-code" data-task-id="${escapeHtml(taskId)}" data-cell-index="${index}">
      <div class="nb-cell-head">
        <div>
          ${title ? `<div class="nb-cell-title">${escapeHtml(title)}</div>` : ""}
          ${kind ? `<div class="nb-cell-kind">${escapeHtml(kind.replace(/_/g, " "))}</div>` : ""}
        </div>
        ${createdAt ? `<span class="nb-cell-date">${escapeHtml(formatDate(createdAt))}</span>` : ""}
      </div>
      <div class="nb-input-row">
        <div class="nb-prompt">In&nbsp;[ ]:</div>
        <pre class="nb-code">${escapeHtml(source)}</pre>
      </div>
      ${outputs.length ? `
        <div class="nb-output-row">
          <div class="nb-prompt">Out[ ]:</div>
          <div class="nb-output-stack">
            ${outputs.map((output) => analysisNotebookOutputHtml(output)).join("")}
          </div>
        </div>
      ` : ""}
    </article>
  `;
}

function analysisNotebookDocumentHtml(payload) {
  const notebook = payload?.notebook && typeof payload.notebook === "object" ? payload.notebook : {};
  const cells = Array.isArray(notebook?.cells) ? notebook.cells : [];
  const downloadPath = String(payload?.download_path || "").trim();
  if (!cells.length) {
    return analysisWorkspaceStateHtml("Notebook is empty", "Run an analysis step to start building the notebook.", "empty");
  }
  return `
    <div class="nb-doc">
      <div class="nb-toolbar">
        <div class="nb-toolbar-copy">
          <strong>Notebook</strong>
          <span>${escapeHtml(`${cells.length} cell${cells.length === 1 ? "" : "s"} · workspace rev ${payload?.workspace_revision || 0}`)}</span>
        </div>
        ${downloadPath ? `<a class="ghost-btn" href="${escapeHtml(downloadPath)}" download>Download .ipynb</a>` : ""}
      </div>
      <div class="nb-doc-stack">
        ${cells.map((cell, index) => analysisNotebookRenderedCellHtml(cell, index)).join("")}
      </div>
    </div>
  `;
}

function analysisWorkspaceHtml(entry) {
  if (!entry || (entry.loading && !entry.data)) {
    return analysisWorkspaceStateHtml("Loading notebook", "Rebuilding the analysis notebook for this conversation.", "loading");
  }
  if (entry.error && !entry.data) {
    return analysisWorkspaceStateHtml("Notebook unavailable", entry.error, "error");
  }
  return analysisNotebookDocumentHtml(entry?.data || {});
}

function scrollAnalysisNotebookToTask(taskId) {
  const normalizedTaskId = String(taskId || "").trim();
  if (!normalizedTaskId || !el.reportContent) return;
  const target = Array.from(el.reportContent.querySelectorAll(".nb-cell"))
    .find((node) => String(node.dataset.taskId || "") === normalizedTaskId);
  if (!target) return;
  target.scrollIntoView({ behavior: "smooth", block: "start" });
}

async function refreshCurrentAnalysisWorkspace({ force = false } = {}) {
  const conversationId = String(state.selectedConversationId || "").trim();
  if (!conversationId || currentConversationMode() !== "analysis") return;
  const existing = state.analysisWorkspaceByConversationId[conversationId];
  if (existing?.loading) return;
  if (!force && existing && existing.data) return;

  const hasExistingData = Boolean(existing?.data);
  state.analysisWorkspaceByConversationId[conversationId] = hasExistingData
    ? { loading: true, error: "", data: existing.data }
    : { loading: true, error: "", data: null };
  if (!hasExistingData) renderReportPanel();

  try {
    const payload = await api(`/api/conversations/${encodeURIComponent(conversationId)}/analysis-notebook`);
    state.analysisWorkspaceByConversationId[conversationId] = { loading: false, error: "", data: payload };
  } catch (err) {
    state.analysisWorkspaceByConversationId[conversationId] = hasExistingData
      ? { loading: false, error: "", data: existing.data }
      : {
          loading: false,
          error: String(err?.message || "Failed to load analysis notebook."),
          data: null,
        };
  }

  renderReportPanel();
}

async function selectAnalysisDataset(datasetId) {
  const conversationId = String(state.selectedConversationId || "").trim();
  const normalizedDatasetId = String(datasetId || "").trim();
  if (!conversationId || !normalizedDatasetId) return;
  await api(`/api/conversations/${encodeURIComponent(conversationId)}/analysis-selection`, {
    method: "POST",
    body: JSON.stringify({ dataset_id: normalizedDatasetId }),
  });
  await refreshCurrentAnalysisWorkspace({ force: true });
  await refreshConversations({ keepSelection: true, skipRender: true });
  renderAll();
}

async function refreshCurrentDatasetVisuals({ force = false } = {}) {
  const taskId = currentVisualsTaskId();
  if (!taskId || state.reportPanelMode !== "visuals") return;
  const existing = state.visualsByTaskId[taskId];
  if (existing?.loading) return;
  if (!force && existing && existing.data) return;

  const hasExistingData = Boolean(existing?.data);
  state.visualsByTaskId[taskId] = hasExistingData
    ? { loading: true, error: "", data: existing.data }
    : { loading: true, error: "", data: null };
  if (!hasExistingData) renderReportPanel();

  try {
    const payload = await api(`/api/tasks/${encodeURIComponent(taskId)}/dataset-visualizations`);
    state.visualsByTaskId[taskId] = { loading: false, error: "", data: payload };
  } catch (err) {
    state.visualsByTaskId[taskId] = hasExistingData
      ? { loading: false, error: "", data: existing.data }
      : {
          loading: false,
          error: String(err?.message || "Failed to load dataset visualizations."),
          data: null,
        };
  }

  renderReportPanel();
}

function renderReportPanel() {
  const iteration = currentReportIteration();
  const conversationMode = currentConversationMode();
  const isAnalysisMode = conversationMode === "analysis";
  const showPanel = Boolean(
    state.selectedConversationId
    && (isAnalysisMode || (iteration && iteration?.report?.has_report))
  );
  const debugTaskId = currentDebugTaskId();
  const debugEntry = debugTaskId ? state.debugByTaskId[debugTaskId] : null;
  const showDebugToggle = Boolean(showPanel && debugTaskId);
  const showDebugPanel = Boolean(showPanel && state.debugOpen && debugTaskId);
  const graphTaskId = currentGraphTaskId();
  const graphEntry = graphTaskId ? state.graphByTaskId[graphTaskId] : null;
  const visualsTaskId = currentVisualsTaskId();
  const visualsEntry = visualsTaskId ? state.visualsByTaskId[visualsTaskId] : null;
  const showGraphMode = Boolean(showPanel && state.reportPanelMode === "graph" && graphTaskId);
  const taskMode = normalizeTaskMode(iteration?.task?.mode || iteration?.report?.mode || "");
  const hasDatasetVisuals = Boolean(iteration?.report?.has_dataset_visualizations);
  const showVisualsMode = Boolean(
    showPanel
    && taskMode === "analysis"
    && state.reportPanelMode === "visuals"
    && visualsTaskId
    && hasDatasetVisuals
  );

  el.workspace.classList.toggle("report-open", showPanel);
  el.reportPanel.classList.toggle("hidden", !showPanel);

  if (!showPanel) {
    clearRenderedReportPanel();
    el.exportPdfBtn.classList.add("hidden");
    el.datasetVisualsBtn.classList.add("hidden");
    el.evidenceGraphBtn.classList.add("hidden");
    el.debugToggleBtn.classList.add("hidden");
    el.reportStatus.classList.add("hidden");
    el.reportStatus.classList.remove("error");
    el.debugPanel.classList.add("hidden");
    el.graphPanel.classList.add("hidden");
    el.visualsPanel.classList.add("hidden");
    setInnerHtmlIfChanged(el.visualsPanel, "");
    setDebugSummaryHtml("", "");
    setDebugJsonText("", "");
    return;
  }

  if (isAnalysisMode) {
    const detail = state.selectedConversationDetail;
    const conversationId = String(state.selectedConversationId || "").trim();
    const workspaceEntry = currentAnalysisWorkspaceEntry();
    const title = conversationTitle(detail?.conversation) || "Analysis Notebook";
    const panelKey = String(state.selectedReportTaskId || conversationId);
    setRenderedReportTitle(panelKey, title);
    el.exportPdfBtn.classList.add("hidden");
    el.datasetVisualsBtn.classList.add("hidden");
    el.evidenceGraphBtn.classList.add("hidden");
    el.debugToggleBtn.classList.add("hidden");
    el.reportStatus.classList.add("hidden");
    el.reportStatus.classList.remove("error");
    el.debugPanel.classList.add("hidden");
    el.graphPanel.classList.add("hidden");
    el.visualsPanel.classList.add("hidden");
    setInnerHtmlIfChanged(el.visualsPanel, "");
    setDebugSummaryHtml("", "");
    setDebugJsonText("", "");
    el.reportContent.classList.remove("hidden");
    setInnerHtmlIfChanged(el.reportContent, analysisWorkspaceHtml(workspaceEntry));
    if (!workspaceEntry || (workspaceEntry.loading && !workspaceEntry.data)) {
      refreshCurrentAnalysisWorkspace().catch((err) => setNotice(`Failed to load notebook: ${err.message}`, true));
    }
    const pendingTaskId = String(state.pendingAnalysisScrollTaskId || state.selectedReportTaskId || "").trim();
    if (pendingTaskId) {
      requestAnimationFrame(() => scrollAnalysisNotebookToTask(pendingTaskId));
      state.pendingAnalysisScrollTaskId = "";
    }
    return;
  }

  const task = iteration.task || {};
  const taskId = String(task.task_id || "");
  const reportMarkdown = String(iteration?.report?.report_markdown || "").trim();
  const reportTaskChanged = state.renderedReportTaskId !== taskId;
  const titleFallback = taskMode === "analysis" ? "Analysis Workspace" : "Research Report";
  setRenderedReportTitle(taskId, String(task.title || task.user_query || titleFallback));
  setRenderedReportMarkdown(taskId, reportMarkdown, { preserveScroll: !reportTaskChanged });
  el.exportPdfBtn.classList.toggle("hidden", !taskId || taskMode === "analysis");
  el.exportPdfBtn.disabled = state.exportingPdf;
  el.datasetVisualsBtn.classList.toggle("hidden", !(taskId && taskMode === "analysis" && hasDatasetVisuals));
  el.datasetVisualsBtn.textContent = showVisualsMode ? "Show Notes" : "Visual Summary";
  el.evidenceGraphBtn.classList.toggle("hidden", !taskId || taskMode === "analysis");
  el.evidenceGraphBtn.textContent = showGraphMode ? "Show Report" : "Evidence Graph";
  el.debugToggleBtn.classList.toggle("hidden", !showDebugToggle);
  el.debugToggleBtn.textContent = state.debugOpen ? "Hide Debug" : "Show Debug";

  const shouldShowStatus = state.reportStatusTaskId === taskId && String(state.reportStatusText || "").trim().length > 0;
  el.reportStatus.classList.toggle("hidden", !shouldShowStatus);
  el.reportStatus.classList.toggle("error", shouldShowStatus && state.reportStatusError);
  el.reportStatus.textContent = shouldShowStatus ? state.reportStatusText : "";

  el.reportContent.classList.toggle("hidden", showGraphMode || showVisualsMode);
  el.graphPanel.classList.toggle("hidden", !showGraphMode);
  el.visualsPanel.classList.toggle("hidden", !showVisualsMode);
  if (showGraphMode) {
    el.graphLegend.innerHTML = graphLegendHtml(graphEntry?.data || state.graphRenderedPayload || null);
    const searchValue = String(state.graphSearchByTaskId[graphTaskId] || "");
    if (el.graphSearchInput.value !== searchValue) el.graphSearchInput.value = searchValue;

    if (!graphEntry || (graphEntry.loading && !graphEntry.data)) {
      destroyGraphInstance();
      renderGraphCanvasState("loading", "Loading graph", "Building the semantic evidence graph for this report.");
      el.graphInspector.innerHTML = graphSummaryInspectorHtml({
        summary: { node_count: 0, edge_count: 0, evidence_count: 0, mixed_edge_count: 0 },
        warnings: [],
      });
      refreshCurrentEvidenceGraph().catch((err) => setNotice(`Failed to load evidence graph: ${err.message}`, true));
    } else if (graphEntry.error && !graphEntry.data) {
      destroyGraphInstance();
      renderGraphCanvasState("error", "Graph unavailable", graphEntry.error);
      el.graphInspector.innerHTML = graphSummaryInspectorHtml({
        summary: { node_count: 0, edge_count: 0, evidence_count: 0, mixed_edge_count: 0 },
        warnings: [graphEntry.error],
      });
    } else {
      const graphPayload = graphEntry.data || {};
      if (state.graphRenderedTaskId !== graphTaskId || state.graphRenderedPayload !== graphPayload) {
        initializeEvidenceGraph(graphTaskId, graphPayload);
      } else if (state.graphCy) {
        state.graphCy.resize();
      }
      updateGraphInspector(graphTaskId);
      const searchTerm = String(state.graphSearchByTaskId[graphTaskId] || "").trim();
      if (searchTerm) applyGraphSearch(graphTaskId, searchTerm, { refit: false });
    }
  }
  if (showVisualsMode) {
    if (!visualsEntry || (visualsEntry.loading && !visualsEntry.data)) {
      setInnerHtmlIfChanged(
        el.visualsPanel,
        visualsPanelStateHtml("Loading visual summary", "Building the analysis dashboard for this task.", "loading"),
      );
      refreshCurrentDatasetVisuals().catch((err) => setNotice(`Failed to load visual summary: ${err.message}`, true));
    } else if (visualsEntry.error && !visualsEntry.data) {
      setInnerHtmlIfChanged(
        el.visualsPanel,
        visualsPanelStateHtml("Visual summary unavailable", visualsEntry.error, "error"),
      );
    } else {
      setInnerHtmlIfChanged(el.visualsPanel, datasetVisualizationsHtml(visualsEntry.data || {}));
    }
  } else {
    setInnerHtmlIfChanged(el.visualsPanel, "");
  }

  el.debugPanel.classList.toggle("hidden", !showDebugPanel);
  if (showDebugPanel) {
    if (!debugEntry || (debugEntry.loading && !debugEntry.data)) {
      setDebugSummaryHtml(debugTaskId, '<div class="debug-empty">Loading workflow debug state...</div>');
      setDebugJsonText(debugTaskId, "");
      return;
    }

    if (debugEntry.error && !debugEntry.data) {
      setDebugSummaryHtml(debugTaskId, `<div class="debug-empty debug-error">${escapeHtml(debugEntry.error)}</div>`);
      setDebugJsonText(debugTaskId, "");
      return;
    }

    const debugPayload = debugEntry.data || {};
    setDebugSummaryHtml(debugTaskId, debugSummaryHtml(debugPayload));
    setDebugJsonText(debugTaskId, JSON.stringify(debugPayload.state || {}, null, 2));
  } else {
    setDebugSummaryHtml("", "");
    setDebugJsonText("", "");
  }
}

function renderAll() {
  const expanded = Boolean(state.selectedConversationId || state.pendingUserMessage || state.clarificationMessage);
  setExpanded(expanded);
  renderModePicker();
  renderSidebar();
  renderTaskHeader();
  renderMessages();
  renderReportPanel();
  setLoading(state.isLoading);
}

async function refreshHealth() {
  try {
    state.health = await api("/api/health");
    if (!state.health.ok) setNotice(state.health.error || "Backend is not ready.", true);
    else setNotice("");
  } catch (err) {
    state.health = { ok: false };
    setNotice(`Health check failed: ${err.message}`, true);
  }
  updateSendVisibility();
}

async function refreshConversations({ keepSelection = true, skipRender = false } = {}) {
  const payload = await api("/api/conversations");
  state.conversations = Array.isArray(payload?.conversations) ? payload.conversations : [];

  if (!keepSelection) {
    state.selectedConversationId = null;
    state.selectedConversationDetail = null;
    state.selectedReportTaskId = null;
    if (!skipRender) renderAll();
    return;
  }

  const stillExists = state.selectedConversationId
    ? state.conversations.some((conversation) => String(conversation.conversation_id || "") === state.selectedConversationId)
    : false;

  if (!stillExists) {
    state.selectedConversationId = null;
    state.selectedConversationDetail = null;
    state.selectedReportTaskId = null;
    if (!skipRender) renderAll();
    return;
  }

  if (state.selectedConversationId) {
    await selectConversation(state.selectedConversationId, { silent: true, skipRender });
  } else {
    if (!skipRender) renderAll();
  }
}

async function selectConversation(conversationId, { silent = false, skipRender = false } = {}) {
  state.selectedConversationId = conversationId;
  if (!conversationId) {
    state.selectedConversationDetail = null;
    state.selectedReportTaskId = null;
    if (!skipRender) renderAll();
    return;
  }
  try {
    const detail = await api(`/api/conversations/${encodeURIComponent(conversationId)}`);
    state.selectedConversationDetail = detail;
    state.selectedInputMode = normalizeTaskMode(detail?.conversation?.mode || state.selectedInputMode);
    const iterations = Array.isArray(detail?.iterations) ? detail.iterations : [];
    for (const it of iterations) {
      const runId = it?.task?.active_run_id;
      if (runId && !state.activeRunIds.has(runId)) {
        try {
          const run = await api(`/api/runs/${encodeURIComponent(runId)}`);
          storeRunData(run);
          const terminal = ["completed", "failed", "awaiting_hitl", "needs_clarification"].includes(String(run?.status || ""));
          if (!terminal) startRunPolling(runId);
        } catch {
          /* run may have finished, ignore */
        }
      }
    }
    const defaultReportTaskId = String(detail?.conversation?.selected_report_task_id || "").trim();
    const latestDone = latestCompletedTaskId(detail);
    if (!state.selectedReportTaskId || !findIteration(detail, state.selectedReportTaskId)) {
      state.selectedReportTaskId = defaultReportTaskId || latestDone || "";
    }
    state.pendingUserMessage = "";
    state.clarificationMessage = "";
    if (!skipRender) renderAll();
    if (normalizeTaskMode(detail?.conversation?.mode || "") === "analysis") {
      refreshCurrentAnalysisWorkspace().catch((err) => setNotice(`Failed to load notebook: ${err.message}`, true));
    }
    refreshCurrentDebugState().catch((err) => setNotice(`Failed to load debug state: ${err.message}`, true));
  } catch (err) {
    if (!silent) setNotice(`Failed to load conversation: ${err.message}`, true);
  }
}

async function exportFinalReportPdf(taskId) {
  if (!taskId || state.exportingPdf) return;
  state.exportingPdf = true;
  setReportStatus(taskId, "Preparing PDF export...");
  renderReportPanel();

  try {
    const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}/report.pdf`, { method: "GET" });
    if (!response.ok) {
      const raw = await response.text();
      let detail = `HTTP ${response.status}`;
      try {
        const parsed = raw ? JSON.parse(raw) : null;
        detail = (parsed && parsed.detail) || (typeof parsed === "string" && parsed) || detail;
      } catch {
        if (raw) detail = raw;
      }
      throw new Error(detail);
    }

    const blob = await response.blob();
    const objectUrl = URL.createObjectURL(blob);
    const contentDisposition = response.headers.get("Content-Disposition") || "";
    const filenameMatch = contentDisposition.match(/filename="?([^";]+)"?/i);
    const filename = filenameMatch?.[1] || `report-${taskId}.pdf`;

    const link = document.createElement("a");
    link.href = objectUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(objectUrl);
    setReportStatus(taskId, "PDF export complete.");
  } catch (err) {
    setReportStatus(taskId, `PDF export failed: ${err.message}`, true);
  } finally {
    state.exportingPdf = false;
    renderReportPanel();
  }
}

function storeRunData(run) {
  if (!run?.run_id) return;
  state.runsByRunId[run.run_id] = run;
  if (run.task_id) {
    state.runsByTaskId[run.task_id] = run;
  } else if (run.run_id === state.pendingRunId) {
    state.runsByTaskId["__pending__"] = run;
  }
}

function stopRunPolling() {
  if (state.pollTimer) {
    clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
  state.activeRunIds.clear();
  state.pollInFlight = false;
  setLoading(false);
}

function ensurePollTimerRunning() {
  if (state.pollTimer || state.activeRunIds.size === 0) return;
  setLoading(true);
  const poll = async () => {
    if (state.pollInFlight) return;
    const ids = Array.from(state.activeRunIds);
    if (ids.length === 0) {
      if (state.pollTimer) {
        clearInterval(state.pollTimer);
        state.pollTimer = null;
      }
      setLoading(false);
      return;
    }
    state.pollInFlight = true;
    try {
      const results = await Promise.allSettled(
        ids.map((runId) => api(`/api/runs/${encodeURIComponent(runId)}`))
      );
      for (let i = 0; i < ids.length; i++) {
        const runId = ids[i];
        const r = results[i];
        if (r.status === "rejected") {
          state.activeRunIds.delete(runId);
          setNotice(`Run polling failed: ${r.reason?.message || "Unknown error"}`, true);
          continue;
        }
        const run = r.value;
        storeRunData(run);
        updateInlineActivityCard(run);
        updateLoadingSpinnerLabel();
        await handleTerminalRunState(run);
      }
    } finally {
      state.pollInFlight = false;
      if (state.activeRunIds.size === 0 && state.pollTimer) {
        clearInterval(state.pollTimer);
        state.pollTimer = null;
        setLoading(false);
      }
    }
  };
  poll();
  state.pollTimer = setInterval(poll, 1000);
}

function startRunPolling(runId) {
  if (!runId) return;
  state.activeRunIds.add(runId);
  ensurePollTimerRunning();
}

async function handleTerminalRunState(run) {
  if (!run) return;
  const status = String(run.status || "");
  const kind = String(run.kind || "");
  const isQueuedFeedbackAck = status === "queued" && kind === "feedback_task";
  const terminalStates = new Set(["completed", "failed", "awaiting_hitl", "needs_clarification"]);
  if (!terminalStates.has(status) && !isQueuedFeedbackAck) return;
  if (state.handlingTerminalRunIds.has(run.run_id)) return;

  state.handlingTerminalRunIds.add(run.run_id);
  try {
    const wasPendingRun = state.pendingRunId === run.run_id;
    const previousSelection = state.selectedConversationId;

    if (run.task_id) {
      state.startingTaskIds.delete(String(run.task_id || "").trim());
    }

    state.activeRunIds.delete(run.run_id);
    if (state.pendingRunId === run.run_id) state.pendingRunId = null;
    if (state.activeRunIds.size === 0 && state.pollTimer) {
      clearInterval(state.pollTimer);
      state.pollTimer = null;
      setLoading(false);
    }

    if (run.task_id) {
      try {
        const taskDetail = await api(`/api/tasks/${encodeURIComponent(run.task_id)}`);
        const conversationId = String(taskDetail?.task?.conversation_id || "").trim() || `conv_${run.task_id}`;
        const sameConversation = previousSelection && previousSelection === conversationId;
        const shouldAutoFocus = wasPendingRun || !previousSelection || sameConversation;

        if (status === "failed") {
          if (shouldAutoFocus) setNotice(run.error || "Run failed.", true);
        } else if (status === "needs_clarification") {
          if (shouldAutoFocus) {
            state.clarificationMessage = run.clarification || "Clarification required before execution.";
            setNotice("");
          }
        } else if (shouldAutoFocus) {
          setNotice("");
        }

        if (shouldAutoFocus) {
          state.selectedConversationId = conversationId;
        }
        await refreshConversations({ keepSelection: true, skipRender: true });
        if (status === "completed" && shouldAutoFocus) {
          state.selectedReportTaskId = run.task_id;
          const isAnalysisMode = normalizeTaskMode(taskDetail?.task?.mode || "") === "analysis";
          state.reportPanelMode = isAnalysisMode ? "report" : (taskDetail?.task?.has_dataset_visualizations ? "visuals" : "report");
          if (isAnalysisMode) {
            state.pendingAnalysisScrollTaskId = run.task_id;
            await refreshCurrentAnalysisWorkspace({ force: true });
          }
        }
      } catch (err) {
        setNotice(`Could not refresh conversations: ${err.message}`, true);
      }
    } else {
      if (status === "failed") {
        setNotice(run.error || "Run failed.", true);
      } else if (status === "needs_clarification") {
        state.clarificationMessage = run.clarification || "Clarification required before execution.";
        setNotice("");
      } else {
        setNotice("");
      }
      try {
        await refreshConversations({ keepSelection: true, skipRender: true });
      } catch (err) {
        setNotice(`Could not refresh conversations: ${err.message}`, true);
      }
    }

    renderAll();
  } finally {
    state.handlingTerminalRunIds.delete(run.run_id);
  }
}

async function submitNewQuery(query, { conversationId = null, parentTaskId = null } = {}) {
  state.pendingUserMessage = String(query || "").trim();
  state.clarificationMessage = "";
  setActivityExpanded("pending", false);
  renderAll();

  const requestBody = { query, mode: currentConversationMode() };
  if (conversationId) requestBody.conversation_id = conversationId;
  if (parentTaskId) requestBody.parent_task_id = parentTaskId;

  const payload = await api("/api/query", {
    method: "POST",
    body: JSON.stringify(requestBody),
  });

  state.pendingRunId = payload.run_id;
  storeRunData(payload);
  renderMessages();
  startRunPolling(payload.run_id);
}

async function submitContinue(taskId) {
  const normalizedTaskId = String(taskId || "").trim();
  if (!normalizedTaskId || state.startingTaskIds.has(normalizedTaskId)) return;
  const detail = state.selectedConversationDetail;
  const iteration = findIteration(detail, normalizedTaskId);
  const task = iteration?.task || null;
  const planVersionId = iteration?.active_plan_version?.version_id || null;
  state.startingTaskIds.add(normalizedTaskId);
  if (task) task.awaiting_hitl = false;
  renderMessages();
  let payload;
  try {
    try {
      payload = await api(`/api/tasks/${encodeURIComponent(normalizedTaskId)}/start`, {
        method: "POST",
        body: JSON.stringify({ plan_version_id: planVersionId }),
      });
    } catch (err) {
      if (String(err?.message || "").trim() !== "Not Found") {
        throw err;
      }
      payload = await api(`/api/tasks/${encodeURIComponent(normalizedTaskId)}/continue`, {
        method: "POST",
        body: JSON.stringify({}),
      });
    }
  } catch (err) {
    state.startingTaskIds.delete(normalizedTaskId);
    renderMessages();
    throw err;
  }
  storeRunData(payload);
  renderMessages();
  startRunPolling(payload.run_id);
}

async function submitFeedback(taskId, message) {
  let payload;
  try {
    payload = await api(`/api/tasks/${encodeURIComponent(taskId)}/feedback`, {
      method: "POST",
      body: JSON.stringify({ message }),
    });
  } catch (err) {
    if (String(err?.message || "").trim() !== "Not Found") throw err;
    payload = await api(`/api/tasks/${encodeURIComponent(taskId)}/revise`, {
      method: "POST",
      body: JSON.stringify({ scope: message }),
    });
  }
  storeRunData(payload);
  renderMessages();
  startRunPolling(payload.run_id);
}

function clearDraft() {
  state.selectedConversationId = null;
  state.selectedConversationDetail = null;
  state.selectedReportTaskId = null;
  state.reportPanelMode = "report";
  state.debugOpen = false;
  state.debugByTaskId = {};
  state.graphByTaskId = {};
  state.visualsByTaskId = {};
  state.analysisWorkspaceByConversationId = {};
  state.graphSearchByTaskId = {};
  state.graphSelectionByTaskId = {};
  state.pendingAnalysisScrollTaskId = "";
  stopRunPolling();
  state.runsByRunId = {};
  state.runsByTaskId = {};
  state.pendingRunId = null;
  state.clarificationMessage = "";
  state.pendingUserMessage = "";
  state.activityExpandedByTask = {};
  setReportStatus(null, "");
  el.promptInput.value = "";
  updateSendVisibility();
  setNotice("");
  renderAll();
}

function bindEvents() {
  el.tasksList.addEventListener("click", (event) => {
    const item = event.target.closest("[data-conversation-id]");
    if (!item) return;
    selectConversation(item.dataset.conversationId).catch((err) => setNotice(err.message, true));
  });

  el.messages.addEventListener("click", (event) => {
    const activityCard = event.target.closest('[data-action="toggle-activity"]');
    if (activityCard) {
      if (activityCard.classList.contains("expanded") && event.target.closest(".activity-details")) return;
      const taskId = String(activityCard.dataset.taskId || "").trim();
      const expanded = !isActivityExpanded(taskId);
      setActivityExpanded(taskId, expanded);
      activityCard.classList.toggle("expanded", expanded);
      activityCard.setAttribute("aria-expanded", expanded ? "true" : "false");
      const details = activityCard.querySelector(".activity-details");
      if (details) details.classList.toggle("hidden", !expanded);
      return;
    }

    const startBtn = event.target.closest('[data-action="checkpoint-start"]');
    if (startBtn) {
      if (startBtn.disabled) return;
      const taskId = String(startBtn.dataset.taskId || "").trim();
      if (!taskId) return;
      startBtn.disabled = true;
      startBtn.textContent = "Starting...";
      submitContinue(taskId).catch((err) => {
        renderMessages();
        setNotice(`Start failed: ${err.message}`, true);
      });
      return;
    }

    const reportCard = event.target.closest('[data-action="open-report"]');
    if (reportCard) {
      const taskId = String(reportCard.dataset.taskId || "").trim();
      if (!taskId) return;
      const scrollTop = el.messages.scrollTop;
      state.selectedReportTaskId = taskId;
      const iteration = findIteration(state.selectedConversationDetail, taskId);
      const isAnalysisMode = normalizeTaskMode(iteration?.task?.mode || iteration?.report?.mode || "") === "analysis";
      state.reportPanelMode = isAnalysisMode
        ? "report"
        : (iteration?.report?.has_dataset_visualizations ? "visuals" : "report");
      if (isAnalysisMode) {
        state.pendingAnalysisScrollTaskId = taskId;
      }
      renderAll();
      refreshCurrentDebugState().catch((err) => setNotice(`Failed to load debug state: ${err.message}`, true));
      el.messages.scrollTop = scrollTop;
    }
  });

  el.messages.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const activityCard = event.target.closest('[data-action="toggle-activity"]');
    if (activityCard) {
      if (activityCard.classList.contains("expanded") && event.target.closest(".activity-details")) return;
      event.preventDefault();
      const taskId = String(activityCard.dataset.taskId || "").trim();
      const expanded = !isActivityExpanded(taskId);
      setActivityExpanded(taskId, expanded);
      activityCard.classList.toggle("expanded", expanded);
      activityCard.setAttribute("aria-expanded", expanded ? "true" : "false");
      const details = activityCard.querySelector(".activity-details");
      if (details) details.classList.toggle("hidden", !expanded);
      return;
    }

    const reportCard = event.target.closest('[data-action="open-report"]');
    if (!reportCard) return;
    event.preventDefault();
    const taskId = String(reportCard.dataset.taskId || "").trim();
    if (!taskId) return;
    const scrollTop = el.messages.scrollTop;
    state.selectedReportTaskId = taskId;
    const iteration = findIteration(state.selectedConversationDetail, taskId);
    const isAnalysisMode = normalizeTaskMode(iteration?.task?.mode || iteration?.report?.mode || "") === "analysis";
    state.reportPanelMode = isAnalysisMode
      ? "report"
      : (iteration?.report?.has_dataset_visualizations ? "visuals" : "report");
    if (isAnalysisMode) {
      state.pendingAnalysisScrollTaskId = taskId;
    }
    renderAll();
    refreshCurrentDebugState().catch((err) => setNotice(`Failed to load debug state: ${err.message}`, true));
    el.messages.scrollTop = scrollTop;
  });

  el.reportContent.addEventListener("click", (event) => {
    const selectBtn = event.target.closest('[data-action="select-analysis-dataset"]');
    if (!selectBtn) return;
    const datasetId = String(selectBtn.dataset.datasetId || "").trim();
    if (!datasetId) return;
    selectAnalysisDataset(datasetId).catch((err) => setNotice(`Failed to select dataset: ${err.message}`, true));
  });

  el.composerForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const query = el.promptInput.value.trim();
    if (!query) return;

    const detail = state.selectedConversationDetail;
    const active = latestIteration(detail);

    el.promptInput.value = "";
    updateSendVisibility();
    setNotice("");

    if (active?.task?.awaiting_hitl && active?.task?.task_id) {
      submitFeedback(active.task.task_id, query).catch((err) => setNotice(`Feedback failed: ${err.message}`, true));
      return;
    }

    if (state.selectedConversationId) {
      const anchorTaskId =
        state.selectedReportTaskId
        || String(detail?.conversation?.selected_report_task_id || "").trim()
        || latestCompletedTaskId(detail)
        || "";
      submitNewQuery(query, {
        conversationId: state.selectedConversationId,
        parentTaskId: anchorTaskId || null,
      }).catch((err) => setNotice(`Failed to start query: ${err.message}`, true));
      return;
    }

    submitNewQuery(query).catch((err) => setNotice(`Failed to start query: ${err.message}`, true));
  });

  el.promptInput.addEventListener("input", () => updateSendVisibility());
  el.promptInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      el.composerForm.requestSubmit();
    }
  });

  el.newChatBtn.addEventListener("click", () => clearDraft());

  el.exportPdfBtn.addEventListener("click", () => {
    const iteration = currentReportIteration();
    const taskId = String(iteration?.task?.task_id || "").trim();
    if (!taskId) return;
    exportFinalReportPdf(taskId);
  });

  if (el.modePicker) {
    el.modePicker.addEventListener("click", (event) => {
      const card = event.target.closest("[data-mode]");
      if (!card || state.selectedConversationId) return;
      state.selectedInputMode = normalizeTaskMode(card.dataset.mode || "");
      updatePromptPlaceholder();
      renderModePicker();
    });
  }

  el.datasetVisualsBtn.addEventListener("click", () => {
    state.reportPanelMode = state.reportPanelMode === "visuals" ? "report" : "visuals";
    renderReportPanel();
    if (state.reportPanelMode === "visuals") {
      refreshCurrentDatasetVisuals().catch((err) => setNotice(`Failed to load visual summary: ${err.message}`, true));
    }
  });

  el.evidenceGraphBtn.addEventListener("click", () => {
    state.reportPanelMode = state.reportPanelMode === "graph" ? "report" : "graph";
    renderReportPanel();
    if (state.reportPanelMode === "graph") {
      refreshCurrentEvidenceGraph().catch((err) => setNotice(`Failed to load evidence graph: ${err.message}`, true));
    }
  });

  el.debugToggleBtn.addEventListener("click", () => {
    state.debugOpen = !state.debugOpen;
    renderReportPanel();
    refreshCurrentDebugState().catch((err) => setNotice(`Failed to load debug state: ${err.message}`, true));
  });

  el.graphSearchInput.addEventListener("input", () => {
    const taskId = currentGraphTaskId();
    if (!taskId) return;
    applyGraphSearch(taskId, el.graphSearchInput.value);
  });

  el.graphFitBtn.addEventListener("click", () => {
    if (!state.graphCy) return;
    state.graphCy.fit(undefined, 72);
  });

  el.graphResetBtn.addEventListener("click", () => {
    const taskId = currentGraphTaskId();
    if (!taskId) return;
    if (el.graphSearchInput.value) el.graphSearchInput.value = "";
    state.graphSearchByTaskId[taskId] = "";
    setGraphSelection(taskId, null);
    if (state.graphCy) {
      clearGraphClasses(state.graphCy);
      state.graphCy.fit(undefined, 72);
    }
    updateGraphInspector(taskId);
  });

  const exampleContainer = document.getElementById("exampleQueries");
  if (exampleContainer) {
    exampleContainer.addEventListener("click", (event) => {
      const chip = event.target.closest(".example-query");
      if (!chip) return;
      state.selectedInputMode = normalizeTaskMode(chip.dataset.mode || state.selectedInputMode);
      const query = chip.dataset.query || chip.textContent.trim();
      el.promptInput.value = query;
      renderModePicker();
      updateSendVisibility();
      el.promptInput.focus();
    });
  }
}

async function bootstrap() {
  bindEvents();
  updatePromptPlaceholder();
  updateSendVisibility();
  renderAll();
  await refreshHealth();
  await refreshConversations({ keepSelection: true, skipRender: true });
  renderAll();
}

bootstrap().catch((err) => setNotice(`UI initialization failed: ${err.message}`, true));
