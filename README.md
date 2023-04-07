# Balancebeam
A load balancer forwarding HTTP requests. 
See details in https://reberhardt.com/cs110l/spring-2020/assignments/project-2/.

This repo is for Rust studying purpose.

# Highlight
- Handle ownership, lifetime and Rust synchronizing primitives (`Mutex`, `Arc`, ...).
- Complex data structure such as `Mutex<HashMap<String, Arc<Mutex<Window>>>>`
- Tokio async programming and synchronizations
- Basic understanding of memory model (`AtomicBool`)
- Try to get good performance by preventing unnecessary sychronizations.
