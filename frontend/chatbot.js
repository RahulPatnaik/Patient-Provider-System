/**
 * Chatbot Component - Help assistant for the Patient-Provider System
 * Floating chat button with dialog window
 */

const { useState, useEffect, useRef } = React;

const Chatbot = () => {
    const [isOpen, setIsOpen] = useState(false);
    const [messages, setMessages] = useState([
        {
            role: 'assistant',
            content: 'Hi! I\'m here to help you use the Patient-Provider Validation System. Ask me anything about validating providers, managing patient records, or using the system features.'
        }
    ]);
    const [inputMessage, setInputMessage] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const messagesEndRef = useRef(null);

    // Auto-scroll to bottom when new messages arrive
    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    const sendMessage = async () => {
        if (!inputMessage.trim()) return;

        // Add user message
        const userMessage = { role: 'user', content: inputMessage };
        const newMessages = [...messages, userMessage];
        setMessages(newMessages);
        setInputMessage('');
        setIsLoading(true);

        try {
            // Call chatbot API
            const response = await fetch('http://localhost:8000/api/v1/chatbot/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    messages: newMessages
                })
            });

            if (!response.ok) {
                throw new Error('Failed to get response');
            }

            const data = await response.json();

            // Add assistant response
            setMessages([...newMessages, {
                role: 'assistant',
                content: data.message
            }]);
        } catch (error) {
            console.error('Chatbot error:', error);
            setMessages([...newMessages, {
                role: 'assistant',
                content: 'Sorry, I encountered an error. Please try again or check the API documentation at /docs.'
            }]);
        } finally {
            setIsLoading(false);
        }
    };

    const handleKeyPress = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    };

    const quickQuestions = [
        'How do I validate a provider?',
        'How do I add a patient?',
        'What is OCR upload?',
        'What is the difference between fast and full validation?'
    ];

    const handleQuickQuestion = (question) => {
        setInputMessage(question);
    };

    return (
        <>
            {/* Floating Chat Button */}
            <div
                onClick={() => setIsOpen(!isOpen)}
                style={{
                    position: 'fixed',
                    bottom: '24px',
                    right: '24px',
                    width: '60px',
                    height: '60px',
                    borderRadius: '50%',
                    background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15), 0 0 0 0 rgba(102, 126, 234, 0.4)',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    color: 'white',
                    fontSize: '28px',
                    transition: 'all 0.3s ease',
                    zIndex: 1000,
                    animation: isOpen ? 'none' : 'pulse 2s infinite'
                }}
                onMouseEnter={(e) => {
                    e.currentTarget.style.transform = 'scale(1.1)';
                }}
                onMouseLeave={(e) => {
                    e.currentTarget.style.transform = 'scale(1)';
                }}
                title="Need help?"
            >
                {isOpen ? '✕' : '💬'}
            </div>

            {/* Chat Dialog */}
            {isOpen && (
                <div
                    style={{
                        position: 'fixed',
                        bottom: '100px',
                        right: '24px',
                        width: '380px',
                        height: '600px',
                        background: 'white',
                        borderRadius: '12px',
                        boxShadow: '0 8px 32px rgba(0,0,0,0.12)',
                        display: 'flex',
                        flexDirection: 'column',
                        zIndex: 1000,
                        overflow: 'hidden',
                        animation: 'slideUp 0.3s ease-out'
                    }}
                >
                    {/* Header */}
                    <div
                        style={{
                            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                            color: 'white',
                            padding: '20px',
                            display: 'flex',
                            alignItems: 'center',
                            gap: '12px'
                        }}
                    >
                        <div style={{ fontSize: '24px' }}>🤖</div>
                        <div style={{ flex: 1 }}>
                            <div style={{ fontWeight: 'bold', fontSize: '16px' }}>System Assistant</div>
                            <div style={{ fontSize: '12px', opacity: 0.9 }}>Here to help you</div>
                        </div>
                    </div>

                    {/* Messages Area */}
                    <div
                        style={{
                            flex: 1,
                            overflowY: 'auto',
                            padding: '20px',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: '12px',
                            background: '#f7f9fc'
                        }}
                    >
                        {messages.map((msg, index) => (
                            <div
                                key={index}
                                style={{
                                    display: 'flex',
                                    justifyContent: msg.role === 'user' ? 'flex-end' : 'flex-start'
                                }}
                            >
                                <div
                                    style={{
                                        maxWidth: '80%',
                                        padding: '12px 16px',
                                        borderRadius: msg.role === 'user' ? '12px 12px 4px 12px' : '12px 12px 12px 4px',
                                        background: msg.role === 'user'
                                            ? 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
                                            : 'white',
                                        color: msg.role === 'user' ? 'white' : '#333',
                                        fontSize: '14px',
                                        lineHeight: '1.5',
                                        boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                                        whiteSpace: 'pre-wrap',
                                        wordBreak: 'break-word'
                                    }}
                                >
                                    {msg.content}
                                </div>
                            </div>
                        ))}

                        {/* Quick Questions (show only at start) */}
                        {messages.length === 1 && (
                            <div style={{ marginTop: '12px' }}>
                                <div style={{ fontSize: '12px', color: '#666', marginBottom: '8px' }}>
                                    Quick questions:
                                </div>
                                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                                    {quickQuestions.map((q, idx) => (
                                        <button
                                            key={idx}
                                            onClick={() => handleQuickQuestion(q)}
                                            style={{
                                                background: 'white',
                                                border: '1px solid #e0e0e0',
                                                borderRadius: '8px',
                                                padding: '8px 12px',
                                                fontSize: '13px',
                                                color: '#667eea',
                                                cursor: 'pointer',
                                                textAlign: 'left',
                                                transition: 'all 0.2s'
                                            }}
                                            onMouseEnter={(e) => {
                                                e.target.style.background = '#f0f4ff';
                                                e.target.style.borderColor = '#667eea';
                                            }}
                                            onMouseLeave={(e) => {
                                                e.target.style.background = 'white';
                                                e.target.style.borderColor = '#e0e0e0';
                                            }}
                                        >
                                            {q}
                                        </button>
                                    ))}
                                </div>
                            </div>
                        )}

                        {isLoading && (
                            <div style={{ display: 'flex', justifyContent: 'flex-start' }}>
                                <div
                                    style={{
                                        padding: '12px 16px',
                                        borderRadius: '12px 12px 12px 4px',
                                        background: 'white',
                                        boxShadow: '0 2px 8px rgba(0,0,0,0.08)'
                                    }}
                                >
                                    <div className="typing-indicator">
                                        <span></span>
                                        <span></span>
                                        <span></span>
                                    </div>
                                </div>
                            </div>
                        )}

                        <div ref={messagesEndRef} />
                    </div>

                    {/* Input Area */}
                    <div
                        style={{
                            padding: '16px',
                            borderTop: '1px solid #e0e0e0',
                            background: 'white',
                            display: 'flex',
                            gap: '8px',
                            alignItems: 'center'
                        }}
                    >
                        <input
                            type="text"
                            value={inputMessage}
                            onChange={(e) => setInputMessage(e.target.value)}
                            onKeyPress={handleKeyPress}
                            placeholder="Ask me anything..."
                            disabled={isLoading}
                            style={{
                                flex: 1,
                                padding: '12px',
                                border: '1px solid #e0e0e0',
                                borderRadius: '8px',
                                fontSize: '14px',
                                outline: 'none',
                                transition: 'border-color 0.2s'
                            }}
                            onFocus={(e) => e.target.style.borderColor = '#667eea'}
                            onBlur={(e) => e.target.style.borderColor = '#e0e0e0'}
                        />
                        <button
                            onClick={sendMessage}
                            disabled={!inputMessage.trim() || isLoading}
                            style={{
                                padding: '12px 16px',
                                background: inputMessage.trim() && !isLoading
                                    ? 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
                                    : '#e0e0e0',
                                color: 'white',
                                border: 'none',
                                borderRadius: '8px',
                                cursor: inputMessage.trim() && !isLoading ? 'pointer' : 'not-allowed',
                                fontSize: '20px',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                transition: 'all 0.2s'
                            }}
                            onMouseEnter={(e) => {
                                if (inputMessage.trim() && !isLoading) {
                                    e.target.style.transform = 'scale(1.05)';
                                }
                            }}
                            onMouseLeave={(e) => {
                                e.target.style.transform = 'scale(1)';
                            }}
                        >
                            ➤
                        </button>
                    </div>
                </div>
            )}

            {/* CSS Animations */}
            <style>{`
                @keyframes pulse {
                    0% {
                        box-shadow: 0 4px 12px rgba(0,0,0,0.15), 0 0 0 0 rgba(102, 126, 234, 0.4);
                    }
                    50% {
                        box-shadow: 0 4px 12px rgba(0,0,0,0.15), 0 0 0 10px rgba(102, 126, 234, 0);
                    }
                    100% {
                        box-shadow: 0 4px 12px rgba(0,0,0,0.15), 0 0 0 0 rgba(102, 126, 234, 0);
                    }
                }

                @keyframes slideUp {
                    from {
                        transform: translateY(20px);
                        opacity: 0;
                    }
                    to {
                        transform: translateY(0);
                        opacity: 1;
                    }
                }

                .typing-indicator {
                    display: flex;
                    gap: 4px;
                    align-items: center;
                }

                .typing-indicator span {
                    width: 8px;
                    height: 8px;
                    border-radius: 50%;
                    background: #667eea;
                    animation: typing 1.4s infinite;
                }

                .typing-indicator span:nth-child(1) {
                    animation-delay: 0s;
                }

                .typing-indicator span:nth-child(2) {
                    animation-delay: 0.2s;
                }

                .typing-indicator span:nth-child(3) {
                    animation-delay: 0.4s;
                }

                @keyframes typing {
                    0%, 60%, 100% {
                        transform: translateY(0);
                        opacity: 0.4;
                    }
                    30% {
                        transform: translateY(-10px);
                        opacity: 1;
                    }
                }
            `}</style>
        </>
    );
};

// Export for use in other files
if (typeof window !== 'undefined') {
    window.Chatbot = Chatbot;
}
