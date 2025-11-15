import { useState } from 'react';
import type { ReactNode } from 'react';
import './App.css';

type AttributeValue = string | number | boolean | null | { [key: string]: AttributeValue } | AttributeValue[];

type GraphNode = {
  id: string;
  labels: string[];
  attributes: Record<string, AttributeValue>;
};

type GraphEdge = {
  id: string;
  start: string;
  end: string;
  label?: string;
};

type ProcedureRow = Record<string, AttributeValue>;

type ProcedureResult = {
  name: string;
  rows: ProcedureRow[];
};

type PathResult = {
  alias: string;
  nodes: GraphNode[];
  edge_ids: string[];
  length: number;
};

type PathPairResult = {
  start_alias: string;
  end_alias: string;
  start: GraphNode;
  end: GraphNode;
  length: number;
};

type QuerySuccess = {
  status: 'ok';
  messages: string[];
  selected_nodes: GraphNode[];
  procedures: ProcedureResult[];
  paths: PathResult[];
  path_pairs: PathPairResult[];
  rows: Record<string, unknown>[];
  plan_summary?: Record<string, unknown> | null;
};

type QueryError = {
  status: 'error';
  error: string;
};

type ResultSectionVariant = 'rows' | 'nodes' | 'procedures' | 'paths' | 'pairs' | 'plan';

type ResultSection = {
  id: string;
  title: string;
  variant: ResultSectionVariant;
  content: ReactNode;
};

type ResultEntry = {
  id: string;
  query: string;
  timestamp: number;
  requestId?: string | null;
  result: QuerySuccess;
};

type GraphData = {
  nodes: GraphNode[];
  edges: GraphEdge[];
};

const MAX_FEED_ENTRIES = 10;

const formatCellValue = (value: unknown) => {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
};

const buildRowsTable = (rows: Record<string, unknown>[]) => {
  if (!rows.length) {
    return <p className="muted">No rows returned.</p>;
  }
  const columns = Array.from(
    rows.reduce((acc, row) => {
      Object.keys(row).forEach((key) => acc.add(key));
      return acc;
    }, new Set<string>())
  );

  return (
    <div className="table-scroll">
      <table className="rows-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx}>
              {columns.map((column) => (
                <td key={column}>{formatCellValue(row[column])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const toGraphNode = (value: unknown): GraphNode | null => {
  if (!value || typeof value !== 'object') {
    return null;
  }
  const candidate = value as Partial<GraphNode>;
  if (typeof candidate.id === 'string' && Array.isArray(candidate.labels) && candidate.attributes && typeof candidate.attributes === 'object') {
    return candidate as GraphNode;
  }
  return null;
};

const toGraphEdge = (value: unknown): GraphEdge | null => {
  if (!value || typeof value !== 'object') {
    return null;
  }
  const candidate = value as Record<string, unknown>;
  const start = typeof candidate.start === 'string'
    ? candidate.start
    : typeof candidate.from === 'string'
      ? candidate.from
      : typeof candidate.source === 'string'
        ? candidate.source
        : null;
  const end = typeof candidate.end === 'string'
    ? candidate.end
    : typeof candidate.to === 'string'
      ? candidate.to
      : typeof candidate.target === 'string'
        ? candidate.target
        : null;
  if (!start || !end) {
    return null;
  }
  const label = typeof candidate.label === 'string'
    ? candidate.label
    : typeof candidate.type === 'string'
      ? candidate.type
      : undefined;
  const id = typeof candidate.id === 'string' ? candidate.id : `${start}->${end}-${label ?? 'rel'}`;
  return { id, start, end, label };
};

const extractGraphDataFromRow = (row: Record<string, unknown>, rowIndex: number): GraphData | null => {
  const nodesMap = new Map<string, GraphNode>();
  const edgesMap = new Map<string, GraphEdge>();

  const inspectValue = (value: unknown) => {
    const node = toGraphNode(value);
    if (node) {
      nodesMap.set(node.id, node);
      return;
    }
    const edge = toGraphEdge(value);
    if (edge) {
      edgesMap.set(edge.id, edge);
      return;
    }
    if (Array.isArray(value)) {
      value.forEach(inspectValue);
      return;
    }
    if (value && typeof value === 'object') {
      const obj = value as Record<string, unknown>;
      if (Array.isArray(obj.nodes)) {
        obj.nodes.forEach(inspectValue);
      }
      if (Array.isArray(obj.edges)) {
        obj.edges.forEach(inspectValue);
      }
      if (Array.isArray(obj.relationships)) {
        obj.relationships.forEach(inspectValue);
      }
    }
  };

  Object.values(row).forEach(inspectValue);

  const nodes = Array.from(nodesMap.values());
  if (!nodes.length) {
    return null;
  }

  let edges = Array.from(edgesMap.values());
  if (!edges.length && nodes.length > 1) {
    edges = nodes.slice(1).map((node, idx) => ({
      id: `auto-${rowIndex}-${node.id}`,
      start: nodes[idx].id,
      end: node.id,
    }));
  }

  return { nodes, edges };
};

const RowsSectionContent = ({ rows }: { rows: Record<string, unknown>[] }) => {
  const graphPreviews = rows
    .map((row, idx) => ({ graph: extractGraphDataFromRow(row, idx), idx }))
    .filter((entry) => entry.graph !== null) as { graph: GraphData; idx: number }[];

  return (
    <div className="rows-section-content">
      {buildRowsTable(rows)}
      {graphPreviews.length > 0 && (
        <div className="row-graphs">
          {graphPreviews.map(({ graph, idx }) => (
            <RowGraphPreview key={`row-graph-${idx}`} rowIndex={idx} graph={graph} />
          ))}
        </div>
      )}
    </div>
  );
};

type RowGraphPreviewProps = {
  rowIndex: number;
  graph: GraphData;
};

const RowGraphPreview = ({ rowIndex, graph }: RowGraphPreviewProps) => {
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(graph.nodes[0]?.id ?? null);
  const width = 320;
  const height = 220;
  const centerX = width / 2;
  const centerY = height / 2;
  const radius = Math.min(width, height) / 2 - 30;

  const positionedNodes = graph.nodes.map((node, idx) => {
    if (graph.nodes.length === 1) {
      return { node, x: centerX, y: centerY };
    }
    const angle = (2 * Math.PI * idx) / graph.nodes.length;
    const x = centerX + radius * Math.cos(angle);
    const y = centerY + radius * Math.sin(angle);
    return { node, x, y };
  });

  const nodePositionMap = new Map(positionedNodes.map(({ node, x, y }) => [node.id, { x, y }]));
  const selectedNode = selectedNodeId ? graph.nodes.find((node) => node.id === selectedNodeId) ?? null : null;

  return (
    <div className="row-graph-preview">
      <div className="row-graph-header">Graph view for row {rowIndex + 1}</div>
      <div className="graph-visual">
        <div className="graph-canvas">
          <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Graph preview">
            <defs>
              <marker id="arrow" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto" markerUnits="strokeWidth">
                <path d="M0,0 L0,6 L6,3 z" fill="rgba(255,255,255,0.5)" />
              </marker>
            </defs>
            {graph.edges.map((edge) => {
              const start = nodePositionMap.get(edge.start);
              const end = nodePositionMap.get(edge.end);
              if (!start || !end) {
                return null;
              }
              return (
                <line
                  key={edge.id}
                  className="graph-edge"
                  x1={start.x}
                  y1={start.y}
                  x2={end.x}
                  y2={end.y}
                  markerEnd="url(#arrow)"
                />
              );
            })}
            {positionedNodes.map(({ node, x, y }) => {
              const primaryLabel = node.labels?.[0] ?? 'Node';
              const isSelected = node.id === selectedNodeId;
              return (
                <g key={node.id} className={`graph-node${isSelected ? ' selected' : ''}`} onClick={() => setSelectedNodeId(node.id)}>
                  <circle cx={x} cy={y} r={20}>
                    <title>{node.id}</title>
                  </circle>
                  <text x={x} y={y + 4}>{primaryLabel}</text>
                </g>
              );
            })}
          </svg>
        </div>
        <div className="graph-node-detail">
          {selectedNode ? (
            <>
              <h4>{selectedNode.labels?.[0] ?? 'Node'}</h4>
              <p className="mono small">{selectedNode.id}</p>
              <pre>{JSON.stringify(selectedNode.attributes, null, 2)}</pre>
            </>
          ) : (
            <p className="muted">Click a node to inspect its attributes.</p>
          )}
        </div>
      </div>
    </div>
  );
};

const renderNodesSection = (nodes: GraphNode[]) => {
  return (
    <div className="nodes-container">
      {nodes.map((node) => (
        <details key={node.id} className="node-card">
          <summary>
            <span className="node-id">{node.id}</span>
            {node.labels.length > 0 && <span className="node-labels">{node.labels.join(', ')}</span>}
          </summary>
          <pre>{JSON.stringify(node.attributes, null, 2)}</pre>
        </details>
      ))}
    </div>
  );
};

const renderProceduresSection = (procedures: ProcedureResult[]) => (
  <div className="procedures-container">
    {procedures.map((procedure) => (
      <details key={procedure.name} className="procedure-card" open>
        <summary>{procedure.name} ({procedure.rows.length} row(s))</summary>
        {procedure.rows.length === 0 ? (
          <p className="muted">No rows</p>
        ) : (
          <pre>{JSON.stringify(procedure.rows, null, 2)}</pre>
        )}
      </details>
    ))}
  </div>
);

const renderPathsSection = (paths: PathResult[]) => (
  <div className="procedures-container">
    {paths.map((path) => (
      <details key={`${path.alias}-${path.edge_ids.join('-')}`} className="procedure-card" open>
        <summary>
          {path.alias} – length {path.length}
        </summary>
        <pre>{JSON.stringify(path.nodes, null, 2)}</pre>
      </details>
    ))}
  </div>
);

const renderPairsSection = (pairs: PathPairResult[]) => (
  <div className="procedures-container">
    {pairs.map((pair, idx) => (
      <details key={`${pair.start_alias}-${pair.end_alias}-${idx}`} className="procedure-card" open>
        <summary>
          {pair.start_alias} → {pair.end_alias} (length {pair.length})
        </summary>
        <pre>{JSON.stringify({ start: pair.start, end: pair.end }, null, 2)}</pre>
      </details>
    ))}
  </div>
);

const renderPlanSection = (plan: Record<string, unknown>) => (
  <pre>{JSON.stringify(plan, null, 2)}</pre>
);

const buildResultSections = (entryId: string, result: QuerySuccess): ResultSection[] => {
  const sections: ResultSection[] = [];
  if (result.rows?.length) {
    sections.push({
      id: `${entryId}-rows`,
      title: 'Rows',
      variant: 'rows',
      content: <RowsSectionContent rows={result.rows} />,
    });
  }

  if (result.selected_nodes?.length) {
    sections.push({
      id: `${entryId}-nodes`,
      title: 'Selected Nodes',
      variant: 'nodes',
      content: renderNodesSection(result.selected_nodes),
    });
  }

  if (result.procedures?.length) {
    sections.push({
      id: `${entryId}-procedures`,
      title: 'Procedures',
      variant: 'procedures',
      content: renderProceduresSection(result.procedures),
    });
  }

  if (result.paths?.length) {
    sections.push({
      id: `${entryId}-paths`,
      title: 'Paths',
      variant: 'paths',
      content: renderPathsSection(result.paths),
    });
  }

  if (result.path_pairs?.length) {
    sections.push({
      id: `${entryId}-pairs`,
      title: 'Node Pairs',
      variant: 'pairs',
      content: renderPairsSection(result.path_pairs),
    });
  }

  if (result.plan_summary) {
    sections.push({
      id: `${entryId}-plan`,
      title: 'Plan',
      variant: 'plan',
      content: renderPlanSection(result.plan_summary),
    });
  }

  return sections;
};

const API_BASE = (import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, '') ?? '');
const SAMPLE_QUERIES = [
  'MATCH (n:Person) RETURN n;',
  'CALL graphdb.nodeClasses();',
  'CALL graphdb.roles();',
  'CALL std.lines("hello world from graphdb") YIELD word;',
];

function App() {
  const [query, setQuery] = useState<string>(SAMPLE_QUERIES[0]);
  const [isRunning, setIsRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [resultFeed, setResultFeed] = useState<ResultEntry[]>([]);
  const [requestId, setRequestId] = useState<string | null>(null);

  const handleSubmit = async () => {
    if (!query.trim()) {
      return;
    }
    setIsRunning(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      });

      const requestIdHeader = response.headers.get('x-request-id');
      setRequestId(requestIdHeader);

      const payload: QuerySuccess | QueryError = await response.json();
      if (!response.ok || payload.status === 'error') {
        throw new Error('error' in payload ? payload.error : 'Query failed');
      }

      const entryId = requestIdHeader ?? `result-${Date.now()}`;
      setResultFeed((prev) => [
        {
          id: entryId,
          query,
          timestamp: Date.now(),
          requestId: requestIdHeader,
          result: payload,
        },
        ...prev,
      ].slice(0, MAX_FEED_ENTRIES));
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown error';
      setError(message);
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="page">
      <header>
        <div className="brand">
          <img src="/img/logo.png" alt="GraphDB logo" className="brand-logo" />
          <div>
            <h1>GraphDB Client</h1>
            <p>Run Cypher-inspired statements against the daemon and inspect catalog procedures.</p>
          </div>
        </div>
        <div className="status-group">
          <div className="status-pill">
            API target: {API_BASE || '(same origin)'}
          </div>
          <div className="status-pill">
            Last request ID: <span className="mono">{requestId ?? '—'}</span>
          </div>
        </div>
      </header>

      <section className="panel">
        <div className="panel-header">
          <h2>Query</h2>
          <div className="sample-buttons">
            {SAMPLE_QUERIES.map((sample) => (
              <button
                key={sample}
                type="button"
                className="pill"
                onClick={() => setQuery(sample)}
              >
                {sample.startsWith('CALL') ? sample : 'Sample MATCH'}
              </button>
            ))}
          </div>
        </div>
        <textarea
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Write a query..."
          spellCheck={false}
        />
        <div className="actions">
          <button type="button" className="primary-button" onClick={handleSubmit} disabled={isRunning}>
            {isRunning ? 'Running…' : 'Run Query'}
          </button>
          {error && <span className="error-text">{error}</span>}
        </div>
      </section>

      <div className="workspace">
        <section className="panel result-panel">
          <div className="panel-header">
            <h2>Results</h2>
          </div>
          <div className="result-body">
            {resultFeed.length === 0 ? (
              <p className="muted">Run a query to see results here.</p>
            ) : (
              resultFeed.map((entry) => {
                const sections = buildResultSections(entry.id, entry.result);
                const executedAt = new Date(entry.timestamp);
                return (
                  <article key={entry.id} className="result-entry">
                    <header className="result-entry-header">
                      <div>
                        <button
                          type="button"
                          className="result-entry-query"
                          onClick={() => setQuery(entry.query)}
                          title="Click to re-run this query"
                        >
                          <code>{entry.query}</code>
                        </button>
                        <div className="result-entry-meta">
                          <span>Ran at {executedAt.toLocaleString()}</span>
                          {entry.requestId && (
                            <span>Request ID: <span className="mono">{entry.requestId}</span></span>
                          )}
                        </div>
                      </div>
                    </header>
                    <div className="result-entry-content">
                      {sections.length ? sections.map((section) => (
                        <article key={section.id} className={`result-block variant-${section.variant}`}>
                          <div className="result-block-title">
                            <h3>{section.title}</h3>
                          </div>
                          <div className="result-block-content">{section.content}</div>
                        </article>
                      )) : (
                        <p className="muted">No structured output returned.</p>
                      )}
                    </div>
                    <footer className="result-entry-footer">
                      {entry.result.messages?.length ? (
                        <div className="result-entry-messages">
                          <span className="messages-label">Messages:</span>
                          {entry.result.messages.map((message, idx) => (
                            <span key={`${entry.id}-message-${idx}`} className="message-pill">{message}</span>
                          ))}
                        </div>
                      ) : (
                        <span className="muted">No messages returned</span>
                      )}
                    </footer>
                  </article>
                );
              })
            )}
          </div>
        </section>
      </div>
    </div>
  );
}

export default App;
