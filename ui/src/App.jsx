import React, { useState, useEffect, useRef } from 'react';

const API_BASE = window.API_BASE_URL || 'http://localhost:8000';

function formatFileSize(bytes) {
  if (!bytes) return '0 B';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
}

export default function App() {
  // Health state
  const [health, setHealth] = useState({
    status: 'checking',
    kafka_connected: false,
    neo4j_connected: false,
  });

  // Upload & File state
  const [selectedFile, setSelectedFile] = useState(null);
  const [dragActive, setDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState(null);
  const [uploadSuccess, setUploadSuccess] = useState(null);
  const fileInputRef = useRef(null);

  // Ingestion Job state
  const [currentJob, setCurrentJob] = useState(null);
  const [jobStatus, setJobStatus] = useState(null);
  const pollIntervalRef = useRef(null);

  // Chat state
  const [chatMessages, setChatMessages] = useState([]);
  const [chatInput, setChatInput] = useState('');
  const [chatLoading, setChatLoading] = useState(false);
  const chatScrollRef = useRef(null);

  // 1. Poll Health
  const checkHealth = async () => {
    try {
      const res = await fetch(`${API_BASE}/health`);
      if (res.ok) {
        const data = await res.json();
        setHealth(data);
      } else {
        setHealth({ status: 'not_ok', kafka_connected: false, neo4j_connected: false });
      }
    } catch {
      setHealth({ status: 'unreachable', kafka_connected: false, neo4j_connected: false });
    }
  };

  useEffect(() => {
    checkHealth();
    const interval = setInterval(checkHealth, 4000);
    return () => clearInterval(interval);
  }, []);

  // 2. Poll Ingestion Status
  useEffect(() => {
    if (!currentJob?.job_id) return;

    const pollStatus = async () => {
      try {
        const res = await fetch(`${API_BASE}/status?job_id=${currentJob.job_id}`);
        if (!res.ok) return;
        const data = await res.json();
        setJobStatus(data);
        if (data.status === 'complete' || data.status === 'failed') {
          clearInterval(pollIntervalRef.current);
        }
      } catch (err) {
        console.error('Status poll error:', err);
      }
    };

    pollStatus();
    pollIntervalRef.current = setInterval(pollStatus, 800);

    return () => {
      if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
    };
  }, [currentJob]);

  // Auto-scroll chat
  useEffect(() => {
    if (chatScrollRef.current) {
      chatScrollRef.current.scrollTop = chatScrollRef.current.scrollHeight;
    }
  }, [chatMessages, chatLoading]);

  // Drag & drop handlers
  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      selectFile(e.dataTransfer.files[0]);
    }
  };

  const selectFile = (file) => {
    setUploadError(null);
    setUploadSuccess(null);
    setSelectedFile(file);
  };

  // Upload handler
  const handleUpload = async () => {
    if (!selectedFile) return;

    setUploading(true);
    setUploadError(null);
    setUploadSuccess(null);

    const formData = new FormData();
    formData.append('file', selectedFile);

    try {
      const res = await fetch(`${API_BASE}/ingest`, {
        method: 'POST',
        body: formData,
      });

      const data = await res.json();

      if (res.status === 202 || res.ok) {
        setCurrentJob({
          job_id: data.job_id,
          filename: selectedFile.name,
          rows_received: data.rows_received,
        });
        setJobStatus({
          job_id: data.job_id,
          status: 'queued',
          rows_total: data.rows_received,
          rows_loaded: 0,
          rows_failed: 0,
        });
        setUploadSuccess(`Uploaded ${selectedFile.name} — Job ID: ${data.job_id} (${data.rows_received} rows queued)`);
      } else {
        setUploadError(data.detail || 'Upload failed');
      }
    } catch (err) {
      setUploadError(err.message || 'Unable to connect to ingestion API');
    } finally {
      setUploading(false);
    }
  };

  // Chat send handler
  const handleSendChat = async (overrideText) => {
    const question = (overrideText || chatInput).trim();
    if (!question || chatLoading) return;

    const userMsg = {
      id: Date.now(),
      role: 'user',
      text: question,
    };

    setChatMessages((prev) => [...prev, userMsg]);
    if (!overrideText) setChatInput('');
    setChatLoading(true);

    try {
      const res = await fetch(`${API_BASE}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question }),
      });

      if (!res.ok) {
        throw new Error(`HTTP error ${res.status}`);
      }

      const data = await res.json();
      const assistantMsg = {
        id: Date.now() + 1,
        role: 'assistant',
        text: data.answer,
        grounded: data.grounded,
        cypher: data.cypher,
        result: data.result,
        showEvidence: false,
      };

      setChatMessages((prev) => [...prev, assistantMsg]);
    } catch (err) {
      setChatMessages((prev) => [
        ...prev,
        {
          id: Date.now() + 1,
          role: 'assistant',
          text: "I couldn't complete that query. Please verify the API connection.",
          grounded: false,
          cypher: '',
          result: [],
          showEvidence: false,
        },
      ]);
    } finally {
      setChatLoading(false);
    }
  };

  const toggleEvidence = (msgId) => {
    setChatMessages((prev) =>
      prev.map((msg) => (msg.id === msgId ? { ...msg, showEvidence: !msg.showEvidence } : msg))
    );
  };

  const calculatePct = () => {
    if (!jobStatus?.rows_total) return 0;
    const done = jobStatus.rows_loaded + jobStatus.rows_failed;
    return Math.min(100, Math.round((done / jobStatus.rows_total) * 100));
  };

  return (
    <div className="app-container">
      {/* Header */}
      <header className="app-header">
        <div className="header-content">
          <div className="brand">
            <div className="brand-title">
              <span>CSV → Kafka → Neo4j</span>
              <span className="brand-badge">Pipeline</span>
            </div>
            <div className="brand-desc">
              Asynchronous event-driven graph loader with structurally grounded question-answering
            </div>
          </div>

          {/* Health indicators */}
          <div className="health-group">
            <div className="health-pill" title="Overall system status">
              <span
                className={`indicator-dot ${
                  health.status === 'ok' ? 'ok' : health.status === 'checking' ? 'checking' : 'bad'
                }`}
              />
              <span>System {health.status === 'ok' ? 'Ready' : health.status}</span>
            </div>
            <div className="health-divider" />
            <div className="health-pill" title="FastAPI endpoint connectivity">
              <span className={`indicator-dot ${health.status !== 'unreachable' ? 'ok' : 'bad'}`} />
              <span>API</span>
            </div>
            <div className="health-divider" />
            <div className="health-pill" title="Apache Kafka broker connection">
              <span className={`indicator-dot ${health.kafka_connected ? 'ok' : 'bad'}`} />
              <span>Kafka</span>
            </div>
            <div className="health-divider" />
            <div className="health-pill" title="Neo4j Graph Database Bolt connection">
              <span className={`indicator-dot ${health.neo4j_connected ? 'ok' : 'bad'}`} />
              <span>Neo4j</span>
            </div>
          </div>
        </div>
      </header>

      {/* Main Container */}
      <main className="main-layout">
        {/* Left Column: Upload & Ingestion Status */}
        <div className="panel">
          {/* Card: Upload CSV */}
          <div className="card">
            <div className="card-title">
              <span>📁 1. Ingest CSV File</span>
            </div>

            <div
              className={`dropzone ${dragActive ? 'active' : ''}`}
              onDragEnter={handleDrag}
              onDragLeave={handleDrag}
              onDragOver={handleDrag}
              onDrop={handleDrop}
              onClick={() => fileInputRef.current?.click()}
            >
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,text/csv"
                style={{ display: 'none' }}
                onChange={(e) => {
                  if (e.target.files?.[0]) selectFile(e.target.files[0]);
                }}
              />
              <div className="dropzone-icon">☁️</div>
              <div className="dropzone-prompt">Drag and drop your CSV file here</div>
              <div className="dropzone-sub">or click to browse from your computer</div>
            </div>

            {selectedFile && (
              <div className="file-details">
                <div>
                  <span className="file-name">{selectedFile.name}</span>
                  <span className="file-size">({formatFileSize(selectedFile.size)})</span>
                </div>
                <button
                  id="uploadBtn"
                  className="btn btn-primary"
                  onClick={handleUpload}
                  disabled={uploading}
                >
                  {uploading ? 'Publishing to Kafka…' : 'Start Ingest'}
                </button>
              </div>
            )}

            {uploadError && (
              <div className="status-banner error" role="alert">
                <span>⚠️ {uploadError}</span>
              </div>
            )}

            {uploadSuccess && (
              <div className="status-banner success" role="status">
                <span>✅ {uploadSuccess}</span>
              </div>
            )}
          </div>

          {/* Card: Ingestion Status */}
          {jobStatus && (
            <div className="card" id="statusCard">
              <div className="card-title">
                <span>⚡ 2. Pipeline Ingestion Progress</span>
              </div>

              <div className="job-header">
                <div className="job-meta">
                  Job ID: <code>{jobStatus.job_id}</code>
                </div>
                <span className={`badge ${jobStatus.status}`}>
                  ● {jobStatus.status}
                </span>
              </div>

              {/* Progress bar */}
              <div className="progress-wrap">
                <div
                  className="progress-fill"
                  style={{ width: `${calculatePct()}%` }}
                />
              </div>

              {/* Metrics */}
              <div className="metrics-grid">
                <div className="metric-box">
                  <div className="metric-label">Total Rows</div>
                  <div className="metric-val">{jobStatus.rows_total ?? 0}</div>
                </div>
                <div className="metric-box">
                  <div className="metric-label">Neo4j Loaded</div>
                  <div className="metric-val loaded">{jobStatus.rows_loaded ?? 0}</div>
                </div>
                <div className="metric-box">
                  <div className="metric-label">Failed</div>
                  <div className="metric-val failed">{jobStatus.rows_failed ?? 0}</div>
                </div>
                <div className="metric-box">
                  <div className="metric-label">Progress</div>
                  <div className="metric-val">{calculatePct()}%</div>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Right Column: Grounded Chatbot */}
        <div className="panel">
          <div className="card chat-card">
            <div className="card-title">
              <span>💬 3. Grounded Chatbot</span>
            </div>

            {/* Chat History */}
            <div className="chat-history" ref={chatScrollRef}>
              {chatMessages.length === 0 ? (
                <div className="empty-chat">
                  <div className="empty-chat-icon">🔍</div>
                  <h3>Ask questions about your uploaded data</h3>
                  <p style={{ marginTop: '0.4rem', fontSize: '0.85rem' }}>
                    {currentJob
                      ? 'Select a prompt below or ask any question based on columns in the dataset.'
                      : 'Upload a CSV to start asking questions.'}
                  </p>

                  <div className="suggestion-chips">
                    <button
                      className="chip"
                      onClick={() => handleSendChat('How many rows are there?')}
                    >
                      "How many rows are there?"
                    </button>
                    <button
                      className="chip"
                      onClick={() => handleSendChat('What columns does this data have?')}
                    >
                      "What columns does this data have?"
                    </button>
                    <button
                      className="chip"
                      onClick={() => handleSendChat('What are the distinct values of group?')}
                    >
                      "What are the distinct values of group?"
                    </button>
                    <button
                      className="chip"
                      onClick={() =>
                        handleSendChat('How many rows belong to the Billing group?')
                      }
                    >
                      "Rows in Billing group?"
                    </button>
                    <button
                      className="chip"
                      onClick={() =>
                        handleSendChat('How many rows have status = Approved?')
                      }
                    >
                      "Rows where status = Approved?" (Unsupported)
                    </button>
                  </div>
                </div>
              ) : (
                chatMessages.map((msg) => (
                  <div key={msg.id} className={`chat-message ${msg.role} ${!msg.grounded && msg.role === 'assistant' ? 'ungrounded' : ''}`}>
                    <div className="bubble">{msg.text}</div>

                    {msg.role === 'assistant' && (
                      <div>
                        <div
                          className={`grounding-tag ${
                            msg.grounded ? 'grounded' : 'ungrounded'
                          }`}
                        >
                          {msg.grounded ? '✓ Grounded in Neo4j' : '⚠ Ungrounded / Unsupported'}
                        </div>

                        {/* Expandable Evidence Drawer */}
                        <div>
                          <button
                            className="evidence-toggle"
                            onClick={() => toggleEvidence(msg.id)}
                          >
                            <span>{msg.showEvidence ? '▼ Hide Evidence' : '▶ Inspect Evidence'}</span>
                          </button>

                          {msg.showEvidence && (
                            <div className="evidence-drawer">
                              <div className="evidence-row">
                                <span className="evidence-label">Grounded State:</span>
                                <span className="evidence-code">
                                  {msg.grounded ? 'true' : 'false'}
                                </span>
                              </div>
                              <div className="evidence-row">
                                <span className="evidence-label">Executed Cypher:</span>
                                <span className="evidence-code">
                                  {msg.cypher || '(No Cypher query executed)'}
                                </span>
                              </div>
                              <div className="evidence-row">
                                <span className="evidence-label">Raw Database Result:</span>
                                <span className="evidence-code">
                                  {JSON.stringify(msg.result, null, 2)}
                                </span>
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                ))
              )}

              {chatLoading && (
                <div className="chat-message assistant">
                  <div className="bubble">
                    <span className="typing-dot" />
                    <span className="typing-dot" />
                    <span className="typing-dot" />
                  </div>
                </div>
              )}
            </div>

            {/* Input Form */}
            <form
              className="chat-input-bar"
              onSubmit={(e) => {
                e.preventDefault();
                handleSendChat();
              }}
            >
              <input
                id="chatInput"
                className="chat-input"
                type="text"
                placeholder="Ask about row counts, column values, or schema…"
                value={chatInput}
                onChange={(e) => setChatInput(e.target.value)}
                disabled={chatLoading}
              />
              <button
                id="chatSendBtn"
                type="submit"
                className="btn btn-primary"
                disabled={!chatInput.trim() || chatLoading}
              >
                Send
              </button>
            </form>
          </div>
        </div>
      </main>
    </div>
  );
}
