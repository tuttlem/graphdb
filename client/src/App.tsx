import { useState } from 'react';
import type { ReactNode } from 'react';
import './App.css';

type AttributeValue = string | number | boolean | null | { [key: string]: AttributeValue } | AttributeValue[];

type GraphNode = {
  id: string;
  labels: string[];
  attributes: Record<string, AttributeValue>;
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
      content: buildRowsTable(result.rows),
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
