# Lya

**Lya** is a lightweight, open-source AI agent written in Rust.

The goal of Lya is to provide a simple, extensible and local-first AI agent capable of interacting with tools and external systems while keeping the architecture lightweight and understandable.

Lya is designed to work with local LLMs through [Ollama](https://ollama.com/).

## Goals

* 🦀 Built in Rust
* 🧠 Support local LLMs
* 🛠️ Tool and function calling
* 🔌 Extensible agent architecture
* 🏠 Local-first and privacy-friendly
* ⚡ Lightweight and fast
* 📖 Easy to understand and extend

## Architecture

Lya is built around a modular agent architecture.

```text
Lya
├── Agent
│   ├── LLM
│   ├── Tools
│   └── Memory
│
└── Providers
    └── Ollama
```

The architecture is intentionally kept simple during the early development stages so that new components can be added without unnecessary complexity.

## Requirements

* Rust
* Ollama
* A compatible local LLM

## Getting Started

Clone the repository:

```bash
git clone https://github.com/Sharkou/Lya.git
cd Lya
```

Install and run Ollama, then make sure a compatible model is available.

Build Lya:

```bash
cargo build
```

Run it:

```bash
cargo run
```

> Lya is currently under active development. APIs, architecture and features may change significantly.

## Roadmap

* [x] Initial Rust project
* [ ] Ollama integration
* [ ] Tool calling
* [ ] Agent loop
* [ ] Conversation context
* [ ] Memory system
* [ ] More LLM providers
* [ ] Configuration system
* [ ] Documentation
* [ ] Stable API

## Contributing

Lya is an open-source project and contributions are welcome.

The project is still in an early stage, so architecture and APIs are expected to evolve.

## License

Lya is licensed under the MIT License.

This means you are free to use, copy, modify, merge, publish, distribute, sublicense, and sell copies of the software, subject to the terms of the license.

See the [`LICENSE`](LICENSE) file for the full license text.
