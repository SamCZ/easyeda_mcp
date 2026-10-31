declare const eda: EasyEdaApi;
import {
  buildSchematicSnapshot,
  findUnconnectedPins,
  getComponentPins,
  listSchematicComponents,
  traceComponent,
  traceNet,
  validateSchematicArea,
  verifyConnections,
  type RawSchematicData,
  type SchematicSnapshot
} from "../../src/schematic/analysis.js";
import {
  PROTOCOL_VERSION,
  evaluateProtocolCompatibility,
  type ProtocolCompatibility
} from "../../src/protocol/messages.js";
import { getBridgeConfig, getBridgeUri } from "./bridgeConfig.js";

type EasyEdaApi = Record<string, any>;

type BridgeCallMessage = {
  kind: "call";
  requestId: string;
  method: string;
  params?: Record<string, any>;
  timeoutMs?: number;
};

type BridgeResultMessage = {
  kind: "result";
  requestId: string;
  result: unknown;
};

type BridgeErrorMessage = {
  kind: "error";
  requestId: string;
  error: {
    code: string;
    message: string;
    details?: unknown;
  };
};

const WS_ID = "easyeda-mcp-bridge";
const EXTENSION_VERSION = "0.1.6";
const bridgeConfig = getBridgeConfig();

type ConnectionPhase = "idle" | "connecting" | "connected" | "blocked";

type ConnectionState = {
  phase: ConnectionPhase;
  attemptIndex: number;
  lastError?: BridgeErrorMessage["error"];
  lastOpenAt?: string;
  lastStatusAt?: string;
  lastHandshakeAt?: string;
  lastAttemptAt?: string;
  connectedOnce: boolean;
  compatibility: ProtocolCompatibility;
  reconnectTimer?: ReturnType<typeof setTimeout>;
  openTimeoutTimer?: ReturnType<typeof setTimeout>;
  heartbeatTimer?: ReturnType<typeof setInterval>;
};

const connectionState: ConnectionState = {
  phase: "idle",
  attemptIndex: 0,
  connectedOnce: false,
  compatibility: evaluateProtocolCompatibility(PROTOCOL_VERSION)
};

const handlers: Record<string, (params: Record<string, any>) => Promise<unknown> | unknown> = {
  getContext,
  findComponent,
  findNet,
  schematicSnapshot,
  listSchematicComponents: listSchematicComponentsTool,
  listSchematicComponentsTool,
  getComponentPins: getComponentPinsTool,
  getComponentPinsTool,
  traceNet: traceNetTool,
  traceNetTool,
  traceComponent: traceComponentTool,
  traceComponentTool,
  findUnconnectedPins: findUnconnectedPinsTool,
  findUnconnectedPinsTool,
  validateSchematicArea: validateSchematicAreaTool,
  validateSchematicAreaTool,
  verifyConnections: verifyConnectionsTool,
  verifyConnectionsTool,
  navigateComponent,
  schEdit,
  libLookup,
  pcbRules,
  pcbEdit,
  navigateRegion,
  zoomBoard,
  exportBom,
  exportNetlist,
  exportGerber,
  exportPdf,
  confirmedAction
};

export function activate(status?: "onStartupFinished", arg?: string): void {
  log("warn", `EasyEDA MCP Bridge activated: ${status ?? "manual"} ${arg ?? ""}`);
  void ensureBridgeConnected({ reason: "activation", manual: false });
}

export function connect(): void {
  void ensureBridgeConnected({ reason: "manual-connect", manual: true, resetAttempts: true });
}

export function reconnect(): void {
  resetConnectionTimers();
  connectionState.phase = "idle";
  connectionState.attemptIndex = 0;
  void ensureBridgeConnected({ reason: "manual-reconnect", manual: true, resetAttempts: true });
}

export async function showStatus(): Promise<void> {
  const diagnostics = await collectDiagnostics();
  showMessage("EasyEDA MCP Bridge Status", formatStatusSummary(diagnostics));
}

export async function runDiagnostics(): Promise<void> {
  const diagnostics = await collectDiagnostics();
  showMessage("EasyEDA MCP Bridge Diagnostics", formatDiagnostics(diagnostics));
}

async function ensureBridgeConnected(options: { reason: string; manual: boolean; resetAttempts?: boolean }): Promise<void> {
  if (options.resetAttempts) {
    connectionState.attemptIndex = 0;
  }

  if (connectionState.phase === "connecting") {
    return;
  }

  if (connectionState.phase === "connected" && !options.manual) {
    return;
  }

  await startBridge(options);
}

async function startBridge(options: { reason: string; manual: boolean }): Promise<void> {
  ensureApi("sys_WebSocket", "register");
  resetConnectionTimers();
  connectionState.phase = "connecting";
  connectionState.lastAttemptAt = new Date().toISOString();

  const wsUri = getBridgeUri(bridgeConfig);
  const attemptIndex = connectionState.attemptIndex;
  const openTimeoutMs = bridgeConfig.openTimeoutMs;
  connectionState.openTimeoutTimer = setTimeout(() => {
    const error = apiError("bridge_open_timeout", `Timed out waiting for EasyEDA MCP Bridge to open ${wsUri}.`);
    handleConnectionFailure(normalizeError(error), {
      manual: options.manual,
      shouldRetry: true
    });
  }, openTimeoutMs);

  try {
    eda.sys_WebSocket.register(
      WS_ID,
      wsUri,
      async (event: MessageEvent<string>) => {
        await handleMessage(event.data);
      },
      async () => {
        connectionState.phase = "connected";
        connectionState.lastOpenAt = new Date().toISOString();
        connectionState.lastHandshakeAt = connectionState.lastOpenAt;
        connectionState.lastError = undefined;
        connectionState.compatibility = evaluateProtocolCompatibility(PROTOCOL_VERSION);
        connectionState.attemptIndex = 0;
        clearOpenTimeout();
        send({
          kind: "hello",
          client: "easyeda-pro-extension",
          version: EXTENSION_VERSION,
          protocolVersion: PROTOCOL_VERSION,
          compatibility: connectionState.compatibility,
          capabilities: detectCapabilities(),
          status: await getStatus()
        });
        startHeartbeat();
        if (!connectionState.connectedOnce || options.manual) {
          connectionState.connectedOnce = true;
          showMessage("EasyEDA MCP Bridge", `Connected to ${wsUri}.`);
        }
      }
    );
  } catch (error) {
    handleConnectionFailure(normalizeError(error), {
      manual: options.manual,
      shouldRetry: true
    });
    return;
  }

  log("warn", `Bridge connection attempt ${attemptIndex + 1} started for ${options.reason} -> ${wsUri}`);
}

async function handleMessage(raw: string): Promise<void> {
  let message: BridgeCallMessage;
  try {
    message = JSON.parse(raw) as BridgeCallMessage;
  } catch (error) {
    log("warn", "Ignored malformed MCP bridge message", error);
    return;
  }

  if (message.kind !== "call" || !message.requestId || !message.method) {
    return;
  }

  try {
    const handler = handlers[message.method];
    if (!handler) {
      throw apiError("unknown_method", `Unsupported MCP bridge method: ${message.method}`);
    }
    const result = await handler(message.params ?? {});
    send({
      kind: "result",
      requestId: message.requestId,
      result
    });
  } catch (error) {
    send({
      kind: "error",
      requestId: message.requestId,
      error: normalizeError(error)
    });
  }
}

function send(message: Record<string, unknown>): void {
  ensureApi("sys_WebSocket", "send");
  try {
    eda.sys_WebSocket.send(WS_ID, JSON.stringify(message));
  } catch (error) {
    handleConnectionFailure(normalizeError(error), {
      manual: false,
      shouldRetry: true
    });
    throw error;
  }
}

async function getStatus(): Promise<Record<string, unknown>> {
  const documentInfo = await optionalCall(() => eda.dmt_SelectControl.getCurrentDocumentInfo());
  return {
    connected: connectionState.phase === "connected",
    connectionState: connectionState.phase,
    extensionVersion: EXTENSION_VERSION,
    protocolVersion: PROTOCOL_VERSION,
    compatibility: connectionState.compatibility,
    capabilities: detectCapabilities(),
    activeDocumentType: inferDocumentType(documentInfo),
    projectName: pickString(documentInfo, ["projectName", "project", "parentName"]),
    documentName: pickString(documentInfo, ["name", "title", "documentName"]),
    message: connectionState.lastError?.message,
    documentInfo: sanitize(documentInfo),
    updatedAt: new Date().toISOString()
  };
}

async function emitStatusUpdate(): Promise<void> {
  send({
    kind: "status",
    compatibility: connectionState.compatibility,
    status: await getStatus()
  });
  connectionState.lastStatusAt = new Date().toISOString();
}

function startHeartbeat(): void {
  if (connectionState.heartbeatTimer) {
    clearInterval(connectionState.heartbeatTimer);
  }
  connectionState.heartbeatTimer = setInterval(() => {
    void emitStatusUpdate().catch((error) => {
      handleConnectionFailure(normalizeError(error), {
        manual: false,
        shouldRetry: true
      });
    });
  }, bridgeConfig.heartbeatIntervalMs);
}

function clearOpenTimeout(): void {
  if (connectionState.openTimeoutTimer) {
    clearTimeout(connectionState.openTimeoutTimer);
    connectionState.openTimeoutTimer = undefined;
  }
}

function resetConnectionTimers(): void {
  clearOpenTimeout();
  if (connectionState.reconnectTimer) {
    clearTimeout(connectionState.reconnectTimer);
    connectionState.reconnectTimer = undefined;
  }
  if (connectionState.heartbeatTimer) {
    clearInterval(connectionState.heartbeatTimer);
    connectionState.heartbeatTimer = undefined;
  }
}

function handleConnectionFailure(
  error: BridgeErrorMessage["error"],
  options: { manual: boolean; shouldRetry: boolean }
): void {
  clearOpenTimeout();
  if (connectionState.heartbeatTimer) {
    clearInterval(connectionState.heartbeatTimer);
    connectionState.heartbeatTimer = undefined;
  }

  connectionState.lastError = error;
  connectionState.compatibility = evaluateProtocolCompatibility(PROTOCOL_VERSION);
  connectionState.phase = isPermissionLikeError(error) ? "blocked" : "idle";

  const hasRetryLeft = options.shouldRetry && connectionState.attemptIndex < bridgeConfig.reconnectDelayMs.length - 1;
  if (hasRetryLeft) {
    const nextAttempt = connectionState.attemptIndex + 1;
    const delayMs = bridgeConfig.reconnectDelayMs[nextAttempt];
    connectionState.attemptIndex = nextAttempt;
    connectionState.reconnectTimer = setTimeout(() => {
      connectionState.reconnectTimer = undefined;
      void ensureBridgeConnected({
        reason: "retry",
        manual: false
      });
    }, delayMs);
  }

  if (options.manual || !hasRetryLeft || connectionState.phase === "blocked") {
    showMessage("EasyEDA MCP Bridge", [
      error.message,
      "",
      ...diagnosticHints(error)
    ].join("\n"));
  }
}

async function collectDiagnostics(): Promise<Record<string, unknown>> {
  const documentInfo = await optionalCall(() => eda.dmt_SelectControl.getCurrentDocumentInfo());
  return {
    bridge: {
      uri: getBridgeUri(bridgeConfig),
      config: bridgeConfig
    },
    websocket: {
      registerAvailable: Boolean(eda.sys_WebSocket?.register),
      sendAvailable: Boolean(eda.sys_WebSocket?.send)
    },
    connection: {
      phase: connectionState.phase,
      connectedOnce: connectionState.connectedOnce,
      lastAttemptAt: connectionState.lastAttemptAt,
      lastOpenAt: connectionState.lastOpenAt,
      lastHandshakeAt: connectionState.lastHandshakeAt,
      lastStatusAt: connectionState.lastStatusAt,
      lastError: connectionState.lastError,
      compatibility: connectionState.compatibility
    },
    activeDocument: {
      available: Boolean(documentInfo),
      type: inferDocumentType(documentInfo),
      projectName: pickString(documentInfo, ["projectName", "project", "parentName"]),
      documentName: pickString(documentInfo, ["name", "title", "documentName"])
    },
    nextSteps: diagnosticHints(connectionState.lastError)
  };
}

function formatStatusSummary(diagnostics: Record<string, unknown>): string {
  const connection = diagnostics.connection as Record<string, unknown>;
  const document = diagnostics.activeDocument as Record<string, unknown>;
  return [
    `Bridge URI: ${getBridgeUri(bridgeConfig)}`,
    `Connection phase: ${String(connection.phase ?? "unknown")}`,
    `Last open: ${String(connection.lastOpenAt ?? "never")}`,
    `Document: ${String(document.documentName ?? document.projectName ?? "none")}`
  ].join("\n");
}

function formatDiagnostics(diagnostics: Record<string, unknown>): string {
  const websocket = diagnostics.websocket as Record<string, unknown>;
  const connection = diagnostics.connection as Record<string, unknown>;
  const document = diagnostics.activeDocument as Record<string, unknown>;
  const nextSteps = Array.isArray(diagnostics.nextSteps) ? diagnostics.nextSteps as string[] : [];

  return [
    `Bridge URI: ${getBridgeUri(bridgeConfig)}`,
    `WebSocket register available: ${String(websocket.registerAvailable)}`,
    `WebSocket send available: ${String(websocket.sendAvailable)}`,
    `Connection phase: ${String(connection.phase ?? "unknown")}`,
    `Last open: ${String(connection.lastOpenAt ?? "never")}`,
    `Last status: ${String(connection.lastStatusAt ?? "never")}`,
    `Compatibility: ${String((connection.compatibility as Record<string, unknown>)?.compatible ?? false)}`,
    `Document: ${String(document.documentName ?? document.projectName ?? "none")}`,
    nextSteps.length > 0 ? `Next steps:\n- ${nextSteps.join("\n- ")}` : "Next steps: none"
  ].join("\n");
}

function diagnosticHints(error?: BridgeErrorMessage["error"]): string[] {
  if (isPermissionLikeError(error)) {
    return [
      "Enable external interaction/WebSocket permission for the extension in EasyEDA Pro.",
      "Reload the extension and let it auto-connect again."
    ];
  }

  if (error?.code === "bridge_open_timeout") {
    return [
      "Make sure the MCP server is running locally.",
      `Confirm the bridge endpoint is reachable at ${getBridgeUri(bridgeConfig)}.`
    ];
  }

  return [
    "Keep EasyEDA Pro open while the MCP server is running.",
    "Use Reconnect or Run Diagnostics from the extension menu if the bridge stays unavailable."
  ];
}

function isPermissionLikeError(error?: BridgeErrorMessage["error"]): boolean {
  const message = `${error?.message ?? ""} ${error?.code ?? ""}`.toLowerCase();
  return message.includes("permission") || message.includes("external interaction") || message.includes("sys_websocket");
}

async function getContext(): Promise<Record<string, unknown>> {
  const documentInfo = await optionalCall(() => eda.dmt_SelectControl.getCurrentDocumentInfo());
  const splitScreenTree = await optionalCall(() => eda.dmt_EditorControl.getSplitScreenTree());
  const pcbNets = await optionalCall(() => getPcbNetNames());
  const pcbComponents = await optionalCall(() => getPcbComponents());
  const schComponents = await optionalCall(() => getSchematicComponents());

  return {
    status: await getStatus(),
    documentInfo: sanitize(documentInfo),
    splitScreenTree: sanitize(splitScreenTree),
    counts: {
      pcbNets: Array.isArray(pcbNets) ? pcbNets.length : undefined,
      pcbComponents: Array.isArray(pcbComponents) ? pcbComponents.length : undefined,
      schematicComponents: Array.isArray(schComponents) ? schComponents.length : undefined
    },
    betaApi: true
  };
}

async function findComponent(params: Record<string, any>): Promise<Record<string, unknown>> {
  const query = String(params.query ?? "").toLowerCase();
  const limit = Number(params.limit ?? 20);
  const pcb = (await optionalCall(() => getPcbComponents())) ?? [];
  const schematic = (await optionalCall(() => getSchematicComponents())) ?? [];
  const all = [
    ...toArray(pcb).map((item) => ({ source: "pcb", item })),
    ...toArray(schematic).map((item) => ({ source: "schematic", item }))
  ];
  const matches = all.filter(({ item }) => JSON.stringify(item).toLowerCase().includes(query)).slice(0, limit);

  return {
    query,
    count: matches.length,
    matches: matches.map(({ source, item }) => ({ source, component: simplifyPrimitive(item) })),
    betaApi: true
  };
}

async function findNet(params: Record<string, any>): Promise<Record<string, unknown>> {
  const query = String(params.query ?? "").toLowerCase();
  const limit = Number(params.limit ?? 20);
  const names = toArray(await getPcbNetNames()).filter((name) => String(name).toLowerCase().includes(query)).slice(0, limit);
  const nets = [];
  for (const name of names) {
    const detail = await optionalCall(() => eda.pcb_Net.getNet(name));
    const length = await optionalCall(() => eda.pcb_Net.getNetLength(name));
    nets.push({
      name,
      detail: sanitize(detail),
      length
    });
  }

  return {
    query,
    count: nets.length,
    nets,
    betaApi: true
  };
}

async function schematicSnapshot(params: Record<string, any>): Promise<SchematicSnapshot> {
  return collectSchematicSnapshot({
    includeRaw: params.includeRaw !== false,
    allPages: params.allPages !== false
  });
}

async function listSchematicComponentsTool(params: Record<string, any>): Promise<Record<string, unknown>> {
  const snapshot = await collectSchematicSnapshot({ includeRaw: params.includeRaw === true, allPages: params.allPages !== false });
  const components = listSchematicComponents(snapshot, stringOrUndefined(params.query), Number(params.limit ?? 100));
  return {
    components,
    count: components.length,
    totalComponents: snapshot.counts.components,
    confidence: snapshot.confidence,
    warnings: snapshot.warnings,
    betaApi: true
  };
}

async function getComponentPinsTool(params: Record<string, any>): Promise<Record<string, unknown>> {
  const snapshot = await collectSchematicSnapshot({ includeRaw: params.includeRaw !== false, allPages: params.allPages !== false });
  const result = getComponentPins(snapshot, String(params.query ?? ""));
  return {
    ...result,
    betaApi: true
  };
}

async function traceNetTool(params: Record<string, any>): Promise<Record<string, unknown>> {
  const snapshot = await collectSchematicSnapshot({ includeRaw: params.includeRaw !== false, allPages: params.allPages !== false });
  const result = traceNet(snapshot, String(params.query ?? ""));
  return {
    ...result,
    betaApi: true
  };
}

async function traceComponentTool(params: Record<string, any>): Promise<Record<string, unknown>> {
  const snapshot = await collectSchematicSnapshot({ includeRaw: params.includeRaw !== false, allPages: params.allPages !== false });
  const result = traceComponent(snapshot, String(params.query ?? ""));
  return {
    ...result,
    betaApi: true
  };
}

async function findUnconnectedPinsTool(params: Record<string, any>): Promise<Record<string, unknown>> {
  const snapshot = await collectSchematicSnapshot({ includeRaw: params.includeRaw !== false, allPages: params.allPages !== false });
  const result = findUnconnectedPins(snapshot, {
    includePowerPins: params.includePowerPins !== false,
    limit: Number(params.limit ?? 100)
  });
  return {
    ...result,
    betaApi: true
  };
}

async function validateSchematicAreaTool(params: Record<string, any>): Promise<Record<string, unknown>> {
  const snapshot = await collectSchematicSnapshot({ includeRaw: params.includeRaw !== false, allPages: params.allPages !== false });
  const result = validateSchematicArea(snapshot, {
    components: Array.isArray(params.components) ? params.components.map(String) : undefined,
    nets: Array.isArray(params.nets) ? params.nets.map(String) : undefined,
    includeGlobalChecks: params.includeGlobalChecks !== false
  });
  return {
    ...result,
    snapshotCounts: snapshot.counts,
    snapshotWarnings: snapshot.warnings,
    betaApi: true
  };
}

async function verifyConnectionsTool(params: Record<string, any>): Promise<Record<string, unknown>> {
  const snapshot = await collectSchematicSnapshot({ includeRaw: params.includeRaw !== false, allPages: params.allPages !== false });
  const result = verifyConnections(snapshot, Array.isArray(params.checks) ? params.checks : [], {
    maxHops: Number(params.maxHops ?? 4)
  });
  return {
    ...result,
    snapshotCounts: snapshot.counts,
    snapshotWarnings: snapshot.warnings,
    betaApi: true
  };
}

async function navigateComponent(params: Record<string, any>): Promise<Record<string, unknown>> {
  const result = await findComponent({ query: params.query, limit: 1 });
  const first = (result.matches as any[])?.[0];
  if (!first) {
    throw apiError("component_not_found", `No component matched "${params.query}".`);
  }

  const primitiveId = first.component.primitiveId ?? first.component.id ?? first.component.uuid;
  if (!primitiveId) {
    return {
      navigated: false,
      reason: "Matched component did not expose a primitive id.",
      match: first,
      betaApi: true
    };
  }

  const primitiveApi = first.source === "pcb" ? eda.pcb_Primitive : eda.sch_Primitive;
  const bbox = await optionalCall(() => primitiveApi.getPrimitivesBBox([primitiveId]));
  const region = normalizeBBox(bbox);
  if (region) {
    await zoomToRegion(region.left, region.right, region.top, region.bottom);
  }

  return {
    navigated: Boolean(region),
    primitiveId,
    region,
    match: first,
    betaApi: true
  };
}

async function navigateRegion(params: Record<string, any>): Promise<Record<string, unknown>> {
  if (isNumber(params.x) && isNumber(params.y)) {
    if (eda.pcb_Document?.navigateToCoordinates) {
      await eda.pcb_Document.navigateToCoordinates(params.x, params.y);
      return { navigated: true, mode: "coordinates", x: params.x, y: params.y };
    }
    if (eda.dmt_EditorControl?.zoomTo) {
      await eda.dmt_EditorControl.zoomTo(params.x, params.y, params.scaleRatio ?? 1);
      return { navigated: true, mode: "zoomTo", x: params.x, y: params.y, betaApi: true };
    }
  }

  for (const key of ["left", "right", "top", "bottom"]) {
    if (!isNumber(params[key])) {
      throw apiError("invalid_region", "Provide either x/y coordinates or left/right/top/bottom region values.");
    }
  }

  await zoomToRegion(params.left, params.right, params.top, params.bottom);
  return {
    navigated: true,
    mode: "region",
    region: {
      left: params.left,
      right: params.right,
      top: params.top,
      bottom: params.bottom
    },
    betaApi: true
  };
}

async function zoomBoard(): Promise<Record<string, unknown>> {
  ensureApi("pcb_Document", "zoomToBoardOutline");
  await eda.pcb_Document.zoomToBoardOutline();
  return {
    zoomed: true,
    betaApi: true
  };
}

async function exportBom(params: Record<string, any>): Promise<Record<string, unknown>> {
  const fileName = params.fileName ?? `easyeda-bom-${timestamp()}`;
  const format = params.format ?? "csv";
  const api = pickManufactureApi(params.scope, "getBomFile");
  const file = await api.getBomFile(fileName, format);
  return saveApiFile(file, `${fileName}.${format}`, true);
}

async function exportNetlist(params: Record<string, any>): Promise<Record<string, unknown>> {
  const fileName = params.fileName ?? `easyeda-netlist-${timestamp()}`;
  const api = pickManufactureApi(params.scope, "getNetlistFile");
  const file = await api.getNetlistFile(fileName, params.netlistType);
  return saveApiFile(file, `${fileName}.net`, true);
}

async function exportGerber(params: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("pcb_ManufactureData", "getGerberFile");
  const fileName = params.fileName ?? `easyeda-gerber-${timestamp()}`;
  const file = await eda.pcb_ManufactureData.getGerberFile(fileName);
  return saveApiFile(file, `${fileName}.zip`, true);
}

async function exportPdf(params: Record<string, any>): Promise<Record<string, unknown>> {
  const fileName = params.fileName ?? `easyeda-export-${timestamp()}`;
  if (params.scope === "schematic" || (!eda.pcb_ManufactureData?.getPdfFile && eda.sch_ManufactureData?.getExportDocumentFile)) {
    const file = await eda.sch_ManufactureData.getExportDocumentFile(fileName, "pdf");
    return saveApiFile(file, `${fileName}.pdf`, true);
  }
  ensureApi("pcb_ManufactureData", "getPdfFile");
  const file = await eda.pcb_ManufactureData.getPdfFile(fileName);
  return saveApiFile(file, `${fileName}.pdf`, true);
}

async function confirmedAction(params: Record<string, any>): Promise<Record<string, unknown>> {
  const action = String(params.action ?? "");
  const documentInfo = await optionalCall(() => eda.dmt_SelectControl.getCurrentDocumentInfo());
  const documentUuid = pickString(documentInfo, ["uuid", "documentUuid", "id"]);

  if (action === "save") {
    if (eda.pcb_Document?.save) {
      await eda.pcb_Document.save(documentUuid);
      return { action, saved: true, documentUuid };
    }
    if (eda.sch_Document?.save) {
      await eda.sch_Document.save(documentUuid);
      return { action, saved: true, documentUuid };
    }
  }

  if (action === "importChanges") {
    ensureApi("pcb_Document", "importChanges");
    await eda.pcb_Document.importChanges(params.uuid ?? documentUuid);
    return { action, imported: true, documentUuid: params.uuid ?? documentUuid };
  }

  if (action === "autoroute") {
    ensureApi("pcb_Document", "importAutoRouteJsonFile");
    if (!params.params?.file) {
      throw apiError("missing_file", "autoroute requires params.file from an EasyEDA-compatible autoroute JSON/SES file.");
    }
    await eda.pcb_Document.importAutoRouteJsonFile(params.params.file);
    return { action, imported: true, betaApi: true };
  }

  if (action === "autolayout") {
    ensureApi("pcb_Document", "importAutoLayoutJsonFile");
    if (!params.params?.file) {
      throw apiError("missing_file", "autolayout requires params.file from an EasyEDA-compatible autolayout JSON file.");
    }
    await eda.pcb_Document.importAutoLayoutJsonFile(params.params.file);
    return { action, imported: true, betaApi: true };
  }

  throw apiError("unsupported_action", `Unsupported or unavailable confirmed action: ${action}.`);
}

function detectCapabilities(): Record<string, boolean> {
  return {
    websocket: Boolean(eda.sys_WebSocket?.register && eda.sys_WebSocket?.send),
    pcbDocument: Boolean(eda.pcb_Document),
    schDocument: Boolean(eda.sch_Document),
    pcbManufactureData: Boolean(eda.pcb_ManufactureData),
    schManufactureData: Boolean(eda.sch_ManufactureData),
    fileSystem: Boolean(eda.sys_FileSystem?.saveFile)
  };
}

async function getPcbComponents(): Promise<unknown[]> {
  ensureApi("pcb_PrimitiveComponent", "getAll");
  return toArray(await eda.pcb_PrimitiveComponent.getAll());
}

async function getSchematicComponents(): Promise<unknown[]> {
  ensureApi("sch_PrimitiveComponent", "getAll");
  return toArray(await eda.sch_PrimitiveComponent.getAll(undefined, true));
}

async function collectSchematicSnapshot(options: { includeRaw: boolean; allPages: boolean }): Promise<SchematicSnapshot> {
  ensureApi("sch_PrimitiveComponent", "getAll");
  const components = toArray(await eda.sch_PrimitiveComponent.getAll(undefined, options.allPages));
  const pinsByComponent: Record<string, unknown[]> = {};

  if (eda.sch_PrimitiveComponent?.getAllPinsByPrimitiveId) {
    for (const component of components) {
      const componentRecord = component && typeof component === "object" ? component as Record<string, unknown> : {};
      const primitiveId = stringOrUndefined(componentRecord.primitiveId ?? componentRecord.id ?? componentRecord.uuid);
      if (!primitiveId) {
        continue;
      }
      const pins = await optionalCall(() => eda.sch_PrimitiveComponent.getAllPinsByPrimitiveId(primitiveId));
      pinsByComponent[primitiveId] = toArray(pins);
    }
  }

  const rawData: RawSchematicData = {
    components,
    pinsByComponent,
    wires: await optionalCall(() => eda.sch_PrimitiveWire?.getAll ? eda.sch_PrimitiveWire.getAll() : []),
    texts: await optionalCall(() => eda.sch_PrimitiveText?.getAll ? eda.sch_PrimitiveText.getAll() : []),
    includeRaw: options.includeRaw
  };
  return buildSchematicSnapshot(rawData);
}

async function getPcbNetNames(): Promise<unknown[]> {
  ensureApi("pcb_Net", "getAllNetsName");
  if (eda.pcb_Net.getAllNetsName) {
    return toArray(await eda.pcb_Net.getAllNetsName());
  }
  return toArray(await eda.pcb_Net.getAllNetName());
}

async function zoomToRegion(left: number, right: number, top: number, bottom: number): Promise<void> {
  if (eda.pcb_Document?.navigateToRegion) {
    await eda.pcb_Document.navigateToRegion(left, right, top, bottom);
    return;
  }
  ensureApi("dmt_EditorControl", "zoomToRegion");
  await eda.dmt_EditorControl.zoomToRegion(left, right, top, bottom);
}

function pickManufactureApi(scope: string | undefined, method: string): any {
  if (scope === "schematic") {
    ensureApi("sch_ManufactureData", method);
    return eda.sch_ManufactureData;
  }
  if (scope === "pcb") {
    ensureApi("pcb_ManufactureData", method);
    return eda.pcb_ManufactureData;
  }
  if (eda.pcb_ManufactureData?.[method]) {
    return eda.pcb_ManufactureData;
  }
  ensureApi("sch_ManufactureData", method);
  return eda.sch_ManufactureData;
}

async function saveApiFile(file: unknown, fallbackFileName: string, betaApi: boolean): Promise<Record<string, unknown>> {
  const normalizedFile = extractFile(file);
  if (!normalizedFile) {
    return {
      saved: false,
      file: sanitize(file),
      reason: "EasyEDA returned a non-File value; returning it without saving.",
      betaApi
    };
  }

  ensureApi("sys_FileSystem", "saveFile");
  const fileName = (normalizedFile as File).name || fallbackFileName;
  const saveResult = await eda.sys_FileSystem.saveFile(normalizedFile, fileName);
  return {
    saved: true,
    fileName,
    saveResult: sanitize(saveResult),
    betaApi
  };
}

function extractFile(value: unknown): unknown {
  if (typeof File !== "undefined" && value instanceof File) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.find((item) => typeof File !== "undefined" && item instanceof File) ?? value[0];
  }
  if (value && typeof value === "object" && "file" in value) {
    return (value as { file: unknown }).file;
  }
  return value;
}

function simplifyPrimitive(value: unknown): Record<string, unknown> {
  const item = sanitize(value) as Record<string, unknown>;
  return {
    primitiveId: item.primitiveId ?? item.id ?? item.uuid,
    designator: item.designator ?? item.name ?? item.displayName ?? item.prefix,
    value: item.value ?? item.comment ?? item.title,
    footprint: item.footprint ?? item.package ?? item.packageName,
    x: item.x,
    y: item.y,
    layer: item.layer,
    raw: item
  };
}

function normalizeBBox(value: unknown): { left: number; right: number; top: number; bottom: number } | undefined {
  const item = Array.isArray(value) ? value[0] : value;
  if (!item || typeof item !== "object") {
    return undefined;
  }
  const box = item as Record<string, unknown>;
  const left = numeric(box.left ?? box.minX ?? box.x1);
  const right = numeric(box.right ?? box.maxX ?? box.x2);
  const top = numeric(box.top ?? box.minY ?? box.y1);
  const bottom = numeric(box.bottom ?? box.maxY ?? box.y2);
  if ([left, right, top, bottom].every((number) => number !== undefined)) {
    return { left, right, top, bottom } as { left: number; right: number; top: number; bottom: number };
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Schematic editing (beta EasyEDA Pro APIs): wires, library devices, net flags.
// Coordinates are schematic units (10 mil); see eda.sch_PrimitiveWire.create docs.
// ---------------------------------------------------------------------------

function callState(target: any, getter: string): unknown {
  if (!target) {
    return undefined;
  }
  const fn = target[getter];
  if (typeof fn === "function") {
    try {
      return fn.call(target);
    } catch {
      return undefined;
    }
  }
  const key = getter.replace(/^getState_/, "");
  return target[key] ?? target[key.charAt(0).toLowerCase() + key.slice(1)];
}

async function libLookup(params: Record<string, any>): Promise<Record<string, unknown>> {
  const lcsc = typeof params.lcsc === "string" ? params.lcsc.trim() : "";
  const query = typeof params.query === "string" ? params.query.trim() : "";
  if (lcsc) {
    ensureApi("lib_Device", "getByLcscIds");
    const found = await eda.lib_Device.getByLcscIds(lcsc, params.libraryUuid, true);
    return { lcsc, matches: sanitize(toArray(found)), betaApi: true };
  }
  ensureApi("lib_Device", "search");
  const found = await eda.lib_Device.search(query, params.libraryUuid, undefined, undefined, params.limit ?? 20, 1);
  return { query, matches: sanitize(toArray(found)), betaApi: true };
}

// ---------------------------------------------------------------------------
// PCB design rules (beta EasyEDA Pro APIs): rule configurations, net classes,
// differential pairs, equal-length groups, DRC, layers and physical stackup.
// The PCB document must be the active tab.
// ---------------------------------------------------------------------------

async function pcbRules(params: Record<string, any>): Promise<Record<string, unknown>> {
  const operations: Array<Record<string, any>> = Array.isArray(params.operations) ? params.operations : [];
  const results: Array<Record<string, unknown>> = [];
  for (let index = 0; index < operations.length; index += 1) {
    const op = operations[index] ?? {};
    const kind = String(op.kind ?? "");
    try {
      const result = await pcbRuleOperation(kind, op);
      results.push({ index, kind, ok: true, result: deepPlain(result) });
    } catch (error) {
      results.push({ index, kind, ok: false, error: normalizeError(error) });
      if (params.stopOnError !== false) {
        break;
      }
    }
  }
  return {
    requested: operations.length,
    completed: results.filter((r) => r.ok).length,
    results,
    betaApi: true
  };
}

function pcbDrc(method: string): any {
  ensureApi("pcb_Drc", method);
  return (eda as any).pcb_Drc;
}

function pcbLayer(method: string): any {
  ensureApi("pcb_Layer", method);
  return (eda as any).pcb_Layer;
}

async function pcbRuleOperation(kind: string, op: Record<string, any>): Promise<unknown> {
  const name = op.name !== undefined ? String(op.name) : "";
  const nets: Array<string> = Array.isArray(op.nets) ? op.nets.map(String) : [];
  const color = op.color ?? null;
  switch (kind) {
    case "get_config": {
      const current = await pcbDrc("getCurrentRuleConfiguration").getCurrentRuleConfiguration();
      return current ?? null;
    }
    case "get_config_name":
      return await pcbDrc("getCurrentRuleConfigurationName").getCurrentRuleConfigurationName();
    case "get_config_by_name":
      return await pcbDrc("getRuleConfiguration").getRuleConfiguration(name);
    case "list_configs":
      return await pcbDrc("getAllRuleConfigurations").getAllRuleConfigurations(op.includeSystem !== false);
    case "overwrite_config":
      if (!op.config || typeof op.config !== "object") {
        throw apiError("invalid_params", "overwrite_config needs config (the object from get_config .config).");
      }
      return await pcbDrc("overwriteCurrentRuleConfiguration").overwriteCurrentRuleConfiguration(op.config);
    case "save_config":
      if (!op.config || typeof op.config !== "object" || !name) {
        throw apiError("invalid_params", "save_config needs config and name.");
      }
      return await pcbDrc("saveRuleConfiguration").saveRuleConfiguration(op.config, name, op.allowOverwrite !== false);
    case "delete_config":
      return await pcbDrc("deleteRuleConfiguration").deleteRuleConfiguration(name);
    case "set_default_config":
      return await pcbDrc("setAsDefaultRuleConfiguration").setAsDefaultRuleConfiguration(name);
    case "get_default_config_name":
      return await pcbDrc("getDefaultRuleConfigurationName").getDefaultRuleConfigurationName();
    case "get_net_rules":
      return await pcbDrc("getNetRules").getNetRules();
    case "overwrite_net_rules":
      if (!Array.isArray(op.netRules)) {
        throw apiError("invalid_params", "overwrite_net_rules needs netRules (array from get_net_rules).");
      }
      return await pcbDrc("overwriteNetRules").overwriteNetRules(op.netRules);
    case "get_net_by_net_rules":
      return await pcbDrc("getNetByNetRules").getNetByNetRules();
    case "overwrite_net_by_net_rules":
      return await pcbDrc("overwriteNetByNetRules").overwriteNetByNetRules(op.rules ?? {});
    case "get_region_rules":
      return await pcbDrc("getRegionRules").getRegionRules();
    case "overwrite_region_rules":
      if (!Array.isArray(op.regionRules)) {
        throw apiError("invalid_params", "overwrite_region_rules needs regionRules (array from get_region_rules).");
      }
      return await pcbDrc("overwriteRegionRules").overwriteRegionRules(op.regionRules);
    case "get_net_classes":
      return await pcbDrc("getAllNetClasses").getAllNetClasses();
    case "create_net_class":
      return await pcbDrc("createNetClass").createNetClass(name, nets, color);
    case "delete_net_class":
      return await pcbDrc("deleteNetClass").deleteNetClass(name);
    case "rename_net_class":
      return await pcbDrc("modifyNetClassName").modifyNetClassName(name, String(op.newName ?? ""));
    case "add_nets_to_class":
      return await pcbDrc("addNetToNetClass").addNetToNetClass(name, nets);
    case "remove_nets_from_class":
      return await pcbDrc("removeNetFromNetClass").removeNetFromNetClass(name, nets);
    case "get_diff_pairs":
      return await pcbDrc("getAllDifferentialPairs").getAllDifferentialPairs();
    case "create_diff_pair":
      return await pcbDrc("createDifferentialPair").createDifferentialPair(name, String(op.positiveNet ?? ""), String(op.negativeNet ?? ""));
    case "delete_diff_pair":
      return await pcbDrc("deleteDifferentialPair").deleteDifferentialPair(name);
    case "get_equal_length_groups":
      return await pcbDrc("getAllEqualLengthNetGroups").getAllEqualLengthNetGroups();
    case "create_equal_length_group":
      return await pcbDrc("createEqualLengthNetGroup").createEqualLengthNetGroup(name, nets, color);
    case "delete_equal_length_group":
      return await pcbDrc("deleteEqualLengthNetGroup").deleteEqualLengthNetGroup(name);
    case "add_nets_to_equal_length_group":
      return await pcbDrc("addNetToEqualLengthNetGroup").addNetToEqualLengthNetGroup(name, nets);
    case "check_drc":
      return await pcbDrc("check").check(op.strict !== false, Boolean(op.userInterface), op.verbose !== false);
    case "realtime_drc": {
      const action = String(op.action ?? "status");
      if (action === "start") {
        return await pcbDrc("startRealTimeDrc").startRealTimeDrc();
      }
      if (action === "stop") {
        return await pcbDrc("stopRealTimeDrc").stopRealTimeDrc();
      }
      return await pcbDrc("getRealTimeDrcStatus").getRealTimeDrcStatus();
    }
    case "get_nets":
      ensureApi("pcb_Net", "getAllNetsName");
      return await (eda as any).pcb_Net.getAllNetsName();
    case "get_net_info":
      ensureApi("pcb_Net", "getAllNets");
      return await (eda as any).pcb_Net.getAllNets();
    case "get_layers":
      return await pcbLayer("getAllLayers").getAllLayers();
    case "get_copper_layer_count":
      return await pcbLayer("getTheNumberOfCopperLayers").getTheNumberOfCopperLayers();
    case "set_copper_layer_count":
      return await pcbLayer("setTheNumberOfCopperLayers").setTheNumberOfCopperLayers(Number(op.count));
    case "modify_layer":
      return await pcbLayer("modifyLayer").modifyLayer(op.layer, op.property ?? {});
    case "get_stackup":
      return await pcbLayer("getCurrentPhysicalStackingConfiguration").getCurrentPhysicalStackingConfiguration();
    case "get_stackup_name":
      return await pcbLayer("getCurrentPhysicalStackingConfigurationName").getCurrentPhysicalStackingConfigurationName();
    case "list_stackups":
      return await pcbLayer("getAllPhysicalStackingConfigurations").getAllPhysicalStackingConfigurations(op.physicalProps);
    case "get_stackup_by_name":
      return await pcbLayer("getPhysicalStackingConfiguration").getPhysicalStackingConfiguration(name, op.physicalProps);
    case "overwrite_stackup":
      if (!op.config || typeof op.config !== "object") {
        throw apiError("invalid_params", "overwrite_stackup needs config (from get_stackup or list_stackups).");
      }
      return await pcbLayer("overwriteCurrentPhysicalStackingConfiguration").overwriteCurrentPhysicalStackingConfiguration(op.config);
    case "edit":
      // Pass-through to the PCB edit operations so older MCP servers can reach them.
      return await pcbEditOperation(String(op.op ?? ""), op);
    default:
      throw apiError("unknown_operation", `Unsupported PCB rule operation: ${kind}`);
  }
}

// ---------------------------------------------------------------------------
// PCB editing (beta EasyEDA Pro APIs): generic API calls, pads of a footprint,
// pad shape changes, constraint / keepout regions, primitives in a rectangle.
// PCB coordinates and sizes are mil unless an operation sets mm: true.
// ---------------------------------------------------------------------------

const MIL_PER_MM = 1000 / 25.4;
const PAD_GETTERS = ["PrimitiveId", "ParentPrimitiveId", "PadNumber", "Net", "Layer", "X", "Y", "Rotation", "Pad", "Hole",
  "HoleOffsetX", "HoleOffsetY", "Metallization", "SpecialPad", "SolderMaskAndPasteMaskExpansion", "PrimitiveLock"];
const COMPONENT_GETTERS = ["PrimitiveId", "Designator", "Layer", "X", "Y", "Rotation", "PrimitiveLock"];
const GENERIC_GETTERS = ["PrimitiveId", "PrimitiveType", "Net", "Layer", "X", "Y", "Width", "Height", "LineWidth", "Rotation",
  "RegionName", "RuleType", "PadNumber", "ParentPrimitiveId", "StartX", "StartY", "EndX", "EndY", "PrimitiveLock"];

async function pcbEdit(params: Record<string, any>): Promise<Record<string, unknown>> {
  const operations: Array<Record<string, any>> = Array.isArray(params.operations) ? params.operations : [];
  const results: Array<Record<string, unknown>> = [];
  for (let index = 0; index < operations.length; index += 1) {
    const op = operations[index] ?? {};
    const kind = String(op.kind ?? "");
    try {
      const result = await pcbEditOperation(kind, op);
      results.push({ index, kind, ok: true, result: deepPlain(result) });
    } catch (error) {
      results.push({ index, kind, ok: false, error: normalizeError(error) });
      if (params.stopOnError !== false) {
        break;
      }
    }
  }
  return {
    requested: operations.length,
    completed: results.filter((r) => r.ok).length,
    results,
    betaApi: true
  };
}

function pcbStateObject(target: any, getters: string[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const getter of getters) {
    const value = callState(target, "getState_" + getter);
    if (value !== undefined) {
      out[getter.charAt(0).toLowerCase() + getter.slice(1)] = sanitize(value);
    }
  }
  return out;
}

async function resolveArg(value: any): Promise<any> {
  if (Array.isArray(value)) {
    const out: any[] = [];
    for (const item of value) {
      out.push(await resolveArg(item));
    }
    return out;
  }
  if (value && typeof value === "object") {
    if (value.$call && typeof value.$call === "object") {
      const call = value.$call;
      return await callEdaApi(String(call.api ?? ""), String(call.method ?? ""), Array.isArray(call.args) ? call.args : []);
    }
    const out: Record<string, any> = {};
    for (const [key, item] of Object.entries(value)) {
      out[key] = await resolveArg(item);
    }
    return out;
  }
  return value;
}

async function callEdaApi(api: string, method: string, args: any[]): Promise<any> {
  if (!/^[A-Za-z0-9_]+$/.test(api) || !/^[A-Za-z0-9_]+$/.test(method)) {
    throw apiError("invalid_params", "api and method must be plain identifiers, for example pcb_PrimitivePad and getAll.");
  }
  ensureApi(api, method);
  const resolved = await resolveArg(args);
  return await (eda as any)[api][method](...resolved);
}

async function findPcbComponent(designator: string): Promise<any> {
  if (!designator) {
    throw apiError("invalid_params", "designator is required.");
  }
  const comps = await getPcbComponents();
  const found = comps.find((c: any) => String(callState(c, "getState_Designator") ?? "") === designator);
  if (!found) {
    throw apiError("not_found", `No PCB component with designator ${designator}.`);
  }
  return found;
}

function toMil(value: unknown, mm: boolean): number {
  const n = Number(value);
  return mm ? n * MIL_PER_MM : n;
}

async function pcbEditOperation(kind: string, op: Record<string, any>): Promise<unknown> {
  const mm = op.mm === true;
  switch (kind) {
    case "call":
      return await callEdaApi(String(op.api ?? ""), String(op.method ?? ""), Array.isArray(op.args) ? op.args : []);
    case "list_apis": {
      const prefix = typeof op.prefix === "string" ? op.prefix : "";
      const out: Record<string, string[]> = {};
      for (const key of Object.keys(eda as any)) {
        if (prefix && !key.startsWith(prefix)) continue;
        const obj = (eda as any)[key];
        if (!obj || (typeof obj !== "object" && typeof obj !== "function")) continue;
        const names = new Set<string>();
        let proto: any = obj;
        while (proto && proto !== Object.prototype && proto !== Function.prototype) {
          for (const n of Object.getOwnPropertyNames(proto)) {
            if (n !== "constructor" && typeof obj[n] === "function") names.add(n);
          }
          proto = Object.getPrototypeOf(proto);
        }
        out[key] = Array.from(names).sort();
      }
      return out;
    }
    case "list_components": {
      const comps = await getPcbComponents();
      return comps.map((c) => pcbStateObject(c, COMPONENT_GETTERS));
    }
    case "component_pads": {
      const comp = await findPcbComponent(String(op.designator ?? ""));
      const id = String(callState(comp, "getState_PrimitiveId") ?? "");
      ensureApi("pcb_PrimitiveComponent", "getAllPinsByPrimitiveId");
      const pads = toArray(await eda.pcb_PrimitiveComponent.getAllPinsByPrimitiveId(id));
      return { component: pcbStateObject(comp, COMPONENT_GETTERS), pads: pads.map((p) => pcbStateObject(p, PAD_GETTERS)) };
    }
    case "modify_pads": {
      ensureApi("pcb_PrimitivePad", "modify");
      const ids: string[] = Array.isArray(op.primitiveIds) ? op.primitiveIds.map(String) : [];
      if (!ids.length || !op.property || typeof op.property !== "object") {
        throw apiError("invalid_params", "modify_pads needs primitiveIds and property (fields of pcb_PrimitivePad.modify).");
      }
      const property = await resolveArg(op.property);
      if (mm && Array.isArray(property.pad)) {
        property.pad = property.pad.map((v: any, i: number) => (i > 0 && typeof v === "number" ? v * MIL_PER_MM : v));
      }
      const out: Array<Record<string, unknown>> = [];
      for (const id of ids) {
        const res = await eda.pcb_PrimitivePad.modify(id, property);
        out.push({ primitiveId: id, ok: Boolean(res), pad: res ? pcbStateObject(res, PAD_GETTERS) : undefined });
      }
      return out;
    }
    case "create_region": {
      ensureApi("pcb_PrimitiveRegion", "create");
      ensureApi("pcb_MathPolygon", "createPolygon");
      let source: any = op.polygon;
      if (Array.isArray(op.rect) && op.rect.length === 4) {
        const [x0, y0, x1, y1] = op.rect.map((v: any) => toMil(v, mm));
        source = ["R", Math.min(x0, x1), Math.max(y0, y1), Math.abs(x1 - x0), Math.abs(y1 - y0), 0, 0];
      }
      if (!source) {
        throw apiError("invalid_params", "create_region needs rect [x0, y0, x1, y1] or polygon (pcb_MathPolygon.createPolygon source array).");
      }
      const polygon = await eda.pcb_MathPolygon.createPolygon(source);
      const ruleType = Array.isArray(op.ruleType) ? op.ruleType.map(Number) : [9];
      const lineWidth = op.lineWidth !== undefined ? toMil(op.lineWidth, mm) : undefined;
      const region = await eda.pcb_PrimitiveRegion.create(op.layer ?? 12, polygon, ruleType, op.name ? String(op.name) : undefined, lineWidth, false);
      return region ? pcbStateObject(region, GENERIC_GETTERS) : null;
    }
    case "list_regions": {
      ensureApi("pcb_PrimitiveRegion", "getAll");
      const regions = toArray(await eda.pcb_PrimitiveRegion.getAll());
      return regions.map((r) => pcbStateObject(r, GENERIC_GETTERS));
    }
    case "primitives_in_rect": {
      ensureApi("pcb_Document", "getPrimitivesInRegion");
      if (!Array.isArray(op.rect) || op.rect.length !== 4) {
        throw apiError("invalid_params", "primitives_in_rect needs rect [x0, y0, x1, y1].");
      }
      const [x0, y0, x1, y1] = op.rect.map((v: any) => toMil(v, mm));
      const prims = toArray(await eda.pcb_Document.getPrimitivesInRegion(Math.min(x0, x1), Math.max(x0, x1), Math.max(y0, y1), Math.min(y0, y1), op.enclosedOnly === true));
      return prims.map((p) => pcbStateObject(p, GENERIC_GETTERS));
    }
    case "delete": {
      const ids: string[] = Array.isArray(op.primitiveIds) ? op.primitiveIds.map(String) : [];
      const api = String(op.api ?? "pcb_Primitive");
      if (!ids.length) {
        throw apiError("invalid_params", "delete needs primitiveIds.");
      }
      ensureApi(api, "delete");
      return await (eda as any)[api].delete(ids);
    }
    default:
      throw apiError("unknown_operation", `Unsupported PCB edit operation: ${kind}`);
  }
}

async function schEdit(params: Record<string, any>): Promise<Record<string, unknown>> {
  const operations: Array<Record<string, any>> = Array.isArray(params.operations) ? params.operations : [];
  const results: Array<Record<string, unknown>> = [];
  for (let index = 0; index < operations.length; index += 1) {
    const op = operations[index] ?? {};
    const kind = String(op.kind ?? "");
    try {
      let result: Record<string, unknown>;
      if (kind === "create_wire") {
        result = await schCreateWire(op);
      } else if (kind === "create_component") {
        result = await schCreateComponent(op);
      } else if (kind === "create_net_flag") {
        result = await schCreateNetFlag(op);
      } else if (kind === "create_net_port") {
        result = await schCreateNetPort(op);
      } else if (kind === "list_attributes") {
        result = await schListAttributes(op);
      } else if (kind === "modify_attribute") {
        result = await schModifyAttribute(op);
      } else if (kind === "create_net_label") {
        result = await schCreateNetLabel(op);
      } else if (kind === "modify_component") {
        result = await schModifyComponent(op);
      } else if (kind === "delete") {
        result = await schDelete(op);
      } else {
        throw apiError("unknown_operation", `Unsupported schematic edit operation: ${kind}`);
      }
      results.push({ index, kind, ok: true, ...result });
    } catch (error) {
      results.push({ index, kind, ok: false, error: normalizeError(error) });
      if (params.stopOnError !== false) {
        break;
      }
    }
  }
  return {
    requested: operations.length,
    completed: results.filter((r) => r.ok).length,
    results,
    betaApi: true
  };
}

async function schCreateWire(op: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("sch_PrimitiveWire", "create");
  if (!Array.isArray(op.line) || op.line.length === 0) {
    throw apiError("invalid_params", "create_wire needs line: [x1, y1, x2, y2, ...] or [[x1, y1, x2, y2], ...].");
  }
  const wire = await eda.sch_PrimitiveWire.create(op.line, op.net ?? undefined, op.color ?? null, op.lineWidth ?? null, null);
  if (!wire) {
    throw apiError("create_failed", "EasyEDA Pro did not create the wire (invalid path, or the points touch different nets).");
  }
  return {
    primitiveId: callState(wire, "getState_PrimitiveId"),
    net: callState(wire, "getState_Net"),
    line: sanitize(callState(wire, "getState_Line"))
  };
}

async function schCreateComponent(op: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("sch_PrimitiveComponent", "create");
  let device = op.device;
  if (!device && op.lcsc) {
    ensureApi("lib_Device", "getByLcscIds");
    const found = toArray(await eda.lib_Device.getByLcscIds(String(op.lcsc), op.libraryUuid, true));
    device = found[0];
    if (!device) {
      throw apiError("device_not_found", `No library device matched LCSC ${op.lcsc}.`);
    }
  }
  if (!device) {
    throw apiError("invalid_params", "create_component needs lcsc (C number) or device {libraryUuid, uuid}.");
  }
  const comp = await eda.sch_PrimitiveComponent.create(
    device,
    Number(op.x),
    Number(op.y),
    op.subPartName ?? undefined,
    Number(op.rotation ?? 0),
    Boolean(op.mirror),
    op.addIntoBom ?? true,
    op.addIntoPcb ?? true
  );
  if (!comp) {
    throw apiError("create_failed", "EasyEDA Pro did not create the component.");
  }
  const primitiveId = String(callState(comp, "getState_PrimitiveId") ?? "");
  const property: Record<string, unknown> = {};
  for (const key of ["designator", "name", "supplier", "supplierId", "manufacturer", "manufacturerId", "uniqueId"]) {
    if (op[key] !== undefined && op[key] !== null) {
      property[key] = String(op[key]);
    }
  }
  if (op.otherProperty && typeof op.otherProperty === "object") {
    property.otherProperty = op.otherProperty;
  }
  if (primitiveId && Object.keys(property).length > 0) {
    ensureApi("sch_PrimitiveComponent", "modify");
    await eda.sch_PrimitiveComponent.modify(primitiveId, property);
  }
  const refreshed = primitiveId ? await optionalCall(() => eda.sch_PrimitiveComponent.get(primitiveId)) : undefined;
  const pins = primitiveId ? await optionalCall(() => eda.sch_PrimitiveComponent.getAllPinsByPrimitiveId(primitiveId)) : undefined;
  return {
    primitiveId,
    designator: callState(refreshed ?? comp, "getState_Designator"),
    x: callState(refreshed ?? comp, "getState_X"),
    y: callState(refreshed ?? comp, "getState_Y"),
    rotation: callState(refreshed ?? comp, "getState_Rotation"),
    pins: sanitize(toArray(pins))
  };
}

async function schCreateNetFlag(op: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("sch_PrimitiveComponent", "createNetFlag");
  const identification = String(op.identification ?? "Ground");
  const flag = await eda.sch_PrimitiveComponent.createNetFlag(
    identification,
    String(op.net ?? ""),
    Number(op.x),
    Number(op.y),
    Number(op.rotation ?? 0),
    Boolean(op.mirror)
  );
  if (!flag) {
    throw apiError("create_failed", "EasyEDA Pro did not create the net flag.");
  }
  return {
    primitiveId: callState(flag, "getState_PrimitiveId"),
    net: callState(flag, "getState_Net"),
    identification
  };
}

async function schCreateNetPort(op: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("sch_PrimitiveComponent", "createNetPort");
  const direction = String(op.direction ?? "BI");
  const port = await eda.sch_PrimitiveComponent.createNetPort(
    direction,
    String(op.net ?? ""),
    Number(op.x),
    Number(op.y),
    Number(op.rotation ?? 0),
    Boolean(op.mirror)
  );
  if (!port) {
    throw apiError("create_failed", "EasyEDA Pro did not create the net port.");
  }
  return {
    primitiveId: callState(port, "getState_PrimitiveId"),
    net: callState(port, "getState_Net"),
    direction
  };
}

async function schListAttributes(op: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("sch_PrimitiveAttribute", "getAll");
  const items = toArray(await eda.sch_PrimitiveAttribute.getAll(op.parentPrimitiveId ?? undefined));
  const keyFilter = typeof op.key === "string" ? op.key : undefined;
  const attributes = items
    .map((a: any) => ({
      primitiveId: callState(a, "getState_PrimitiveId"),
      parentPrimitiveId: callState(a, "getState_ParentPrimitiveId"),
      key: callState(a, "getState_Key"),
      value: callState(a, "getState_Value"),
      x: callState(a, "getState_X"),
      y: callState(a, "getState_Y"),
      rotation: callState(a, "getState_Rotation"),
      keyVisible: callState(a, "getState_KeyVisible"),
      valueVisible: callState(a, "getState_ValueVisible")
    }))
    .filter((a: any) => !keyFilter || String(a.key) === keyFilter);
  return { count: attributes.length, attributes: attributes.slice(0, 2000) };
}

async function schModifyAttribute(op: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("sch_PrimitiveAttribute", "modify");
  const result = await eda.sch_PrimitiveAttribute.modify(String(op.primitiveId ?? ""), op.property ?? {});
  return { primitiveId: op.primitiveId, modified: Boolean(result) };
}

async function schCreateNetLabel(op: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("sch_PrimitiveAttribute", "createNetLabel");
  const label = await eda.sch_PrimitiveAttribute.createNetLabel(Number(op.x), Number(op.y), String(op.net ?? ""));
  if (!label) {
    throw apiError("create_failed", "EasyEDA Pro did not create the net label (the point must lie on a wire).");
  }
  return {
    primitiveId: callState(label, "getState_PrimitiveId"),
    parentPrimitiveId: callState(label, "getState_ParentPrimitiveId"),
    value: callState(label, "getState_Value"),
    x: callState(label, "getState_X"),
    y: callState(label, "getState_Y")
  };
}

async function schModifyComponent(op: Record<string, any>): Promise<Record<string, unknown>> {
  ensureApi("sch_PrimitiveComponent", "modify");
  const result = await eda.sch_PrimitiveComponent.modify(String(op.primitiveId ?? ""), op.property ?? {});
  return { primitiveId: op.primitiveId, modified: Boolean(result) };
}

async function schDelete(op: Record<string, any>): Promise<Record<string, unknown>> {
  const out: Record<string, unknown> = {};
  if (Array.isArray(op.componentIds) && op.componentIds.length > 0) {
    ensureApi("sch_PrimitiveComponent", "delete");
    out.components = sanitize(await eda.sch_PrimitiveComponent.delete(op.componentIds));
  }
  if (Array.isArray(op.wireIds) && op.wireIds.length > 0) {
    ensureApi("sch_PrimitiveWire", "delete");
    out.wires = sanitize(await eda.sch_PrimitiveWire.delete(op.wireIds));
  }
  return out;
}

function ensureApi(objectName: string, methodName: string): void {
  if (!eda[objectName]?.[methodName]) {
    throw apiError("api_unavailable", `EasyEDA Pro API eda.${objectName}.${methodName} is unavailable in this context.`);
  }
}

function apiError(code: string, message: string, details?: unknown): Error & { code: string; details?: unknown } {
  const error = new Error(message) as Error & { code: string; details?: unknown };
  error.code = code;
  error.details = details;
  return error;
}

function normalizeError(error: unknown): BridgeErrorMessage["error"] {
  if (error instanceof Error) {
    const coded = error as Error & { code?: string; details?: unknown };
    return {
      code: coded.code ?? "easyeda_extension_error",
      message: error.message,
      details: sanitize(coded.details)
    };
  }
  return {
    code: "easyeda_extension_error",
    message: String(error)
  };
}

async function optionalCall<T>(fn: () => T | Promise<T>): Promise<T | undefined> {
  try {
    return await fn();
  } catch (error) {
    log("warn", "Optional EasyEDA API call failed", error);
    return undefined;
  }
}

function deepPlain(value: unknown, depth = 0, seen: WeakSet<object> = new WeakSet()): unknown {
  if (depth > 40) {
    return "[MaxDepth]";
  }
  if (value === null || value === undefined || typeof value !== "object") {
    return typeof value === "function" ? undefined : value;
  }
  if (seen.has(value as object)) {
    return "[Circular]";
  }
  seen.add(value as object);
  if (Array.isArray(value)) {
    return value.slice(0, 5000).map((item) => deepPlain(item, depth + 1, seen));
  }
  const output: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value as Record<string, unknown>).slice(0, 2000)) {
    if (typeof item !== "function") {
      output[key] = deepPlain(item, depth + 1, seen);
    }
  }
  return output;
}

function sanitize(value: unknown, depth = 0): unknown {
  if (depth > 4) {
    return "[MaxDepth]";
  }
  if (value === null || value === undefined || typeof value !== "object") {
    return value;
  }
  if (typeof File !== "undefined" && value instanceof File) {
    return {
      name: value.name,
      size: value.size,
      type: value.type
    };
  }
  if (Array.isArray(value)) {
    return value.slice(0, 200).map((item) => sanitize(item, depth + 1));
  }
  const output: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value as Record<string, unknown>).slice(0, 80)) {
    if (typeof item !== "function") {
      output[key] = sanitize(item, depth + 1);
    }
  }
  return output;
}

function pickString(value: unknown, keys: string[]): string | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  const record = value as Record<string, unknown>;
  for (const key of keys) {
    if (typeof record[key] === "string") {
      return record[key] as string;
    }
  }
  return undefined;
}

function stringOrUndefined(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : typeof value === "number" ? String(value) : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function inferDocumentType(documentInfo: unknown): string {
  const record = isRecord(documentInfo) ? documentInfo : undefined;
  const numericType = record
    ? [
      record.documentType,
      record.doctype,
      record.type
    ].find((value) => typeof value === "number")
    : undefined;

  if (numericType === 1) return "schematic";
  if (numericType === 2) return "pcb";

  const raw = JSON.stringify(documentInfo ?? {}).toLowerCase();
  if (raw.includes("pcb")) return "pcb";
  if (raw.includes("sch") || raw.includes("schematic")) return "schematic";
  if (raw.includes("footprint")) return "footprint";
  if (raw.includes("symbol")) return "symbol";
  return "unknown";
}

function toArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : value == null ? [] : [value];
}

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function numeric(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function timestamp(): string {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function log(level: "warn" | "error", message: string, details?: unknown): void {
  if (eda.sys_Log?.[level]) {
    eda.sys_Log[level](`[easyeda-mcp] ${message}`, details);
    return;
  }
  console[level](`[easyeda-mcp] ${message}`, details);
}

function showMessage(title: string, message: string): void {
  if (eda.sys_Dialog?.showInformationMessage) {
    eda.sys_Dialog.showInformationMessage(message, title);
    return;
  }
  log("warn", `${title}: ${message}`);
}
