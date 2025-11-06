import { useMemo, useState } from 'react';
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

type QuerySuccess = {
  status: 'ok';
  messages: string[];
  selected_nodes: GraphNode[];
  procedures: ProcedureResult[];
};

type QueryError = {
  status: 'error';
  error: string;
};

const API_BASE = (import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, '') ?? '');
const SAMPLE_QUERIES = [
  'SELECT MATCH (n:Person) RETURN n;',
  'CALL graphdb.nodeClasses();',
  'CALL graphdb.roles();',
];

function App() {
  const [query, setQuery] = useState<string>(SAMPLE_QUERIES[0]);
  const [isRunning, setIsRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QuerySuccess | null>(null);
  const [history, setHistory] = useState<string[]>([]);

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

      const payload: QuerySuccess | QueryError = await response.json();
      if (!response.ok || payload.status === 'error') {
        throw new Error('error' in payload ? payload.error : 'Query failed');
      }

      setResult(payload);
      setHistory((prev) => [query, ...prev.filter((entry) => entry !== query)].slice(0, 5));
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown error';
      setError(message);
      setResult(null);
    } finally {
      setIsRunning(false);
    }
  };

  const renderedNodes = useMemo(() => {
    if (!result?.selected_nodes?.length) {
      return (
        <p className="muted">No nodes returned.</p>
      );
    }
    return result.selected_nodes.map((node) => (
      <details key={node.id} className="node-card">
        <summary>
          <span className="node-id">{node.id}</span>
          {node.labels.length > 0 && <span className="node-labels">{node.labels.join(', ')}</span>}
        </summary>
        <pre>{JSON.stringify(node.attributes, null, 2)}</pre>
      </details>
    ));
  }, [result?.selected_nodes]);

  return (
    <div className="page">
      <header>
        <div>
          <h1>GraphDB Client</h1>
          <p>Run Cypher-inspired statements against the daemon and inspect catalog procedures.</p>
        </div>
        <div className="status-pill">
          API target: {API_BASE || '(same origin)'}
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
                {sample.startsWith('CALL') ? sample : 'Sample SELECT'}
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
          <button type="button" onClick={handleSubmit} disabled={isRunning}>
            {isRunning ? 'Running…' : 'Run Query'}
          </button>
          {error && <span className="error-text">{error}</span>}
        </div>
      </section>

      <section className="results">
        <div className="panel">
          <div className="panel-header">
            <h2>Messages</h2>
          </div>
          {result?.messages?.length ? (
            <ul className="messages">
              {result.messages.map((message, idx) => (
                <li key={`${message}-${idx}`}>{message}</li>
              ))}
            </ul>
          ) : (
            <p className="muted">No messages yet.</p>
          )}
        </div>

        <div className="panel">
          <div className="panel-header">
            <h2>Selected Nodes</h2>
          </div>
          <div className="nodes-container">{renderedNodes}</div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <h2>Procedures</h2>
          </div>
          {result?.procedures?.length ? (
            result.procedures.map((procedure) => (
              <details key={procedure.name} className="procedure-card" open>
                <summary>{procedure.name} ({procedure.rows.length} row(s))</summary>
                {procedure.rows.length === 0 ? (
                  <p className="muted">No rows</p>
                ) : (
                  <pre>{JSON.stringify(procedure.rows, null, 2)}</pre>
                )}
              </details>
            ))
          ) : (
            <p className="muted">No procedure output</p>
          )}
        </div>

        <div className="panel">
          <div className="panel-header">
            <h2>History</h2>
          </div>
          {history.length ? (
            <ul className="history">
              {history.map((entry, idx) => (
                <li key={`${entry}-${idx}`}>
                  <button type="button" onClick={() => setQuery(entry)}>
                    {entry}
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <p className="muted">Run a query to populate history</p>
          )}
        </div>
      </section>
    </div>
  );
}

export default App;
