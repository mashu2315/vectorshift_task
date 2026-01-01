from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any

app = FastAPI()

# Add CORS middleware to allow frontend to communicate with backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

class Node(BaseModel):
    id: str
    type: str
    position: Dict[str, float]
    data: Dict[str, Any]

class Edge(BaseModel):
    id: str
    source: str
    target: str
    sourceHandle: str = None
    targetHandle: str = None

class PipelineRequest(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

def is_dag(nodes: List[Node], edges: List[Edge]) -> bool:
    """Check if the graph is a Directed Acyclic Graph using Kahn's algorithm"""
    # Empty graph or graph with no edges is a DAG
    if len(nodes) == 0:
        return True
    if len(edges) == 0:
        return True
    
    # Build adjacency list and in-degree count
    graph: Dict[str, List[str]] = {}
    in_degree: Dict[str, int] = {}
    
    # Initialize all nodes
    for node in nodes:
        graph[node.id] = []
        in_degree[node.id] = 0
    
    # Build graph from edges (only process edges where both nodes exist)
    for edge in edges:
        if edge.source in graph and edge.target in graph:
            graph[edge.source].append(edge.target)
            in_degree[edge.target] = in_degree.get(edge.target, 0) + 1
    
    # Find all nodes with in-degree 0
    queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
    visited_count = 0
    
    # Process nodes using Kahn's algorithm
    while queue:
        node_id = queue.pop(0)
        visited_count += 1
        
        # Reduce in-degree of neighbors
        for neighbor in graph.get(node_id, []):
            if neighbor in in_degree:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
    
    # If we visited all nodes, it's a DAG
    # If we couldn't visit all nodes, there's a cycle (not a DAG)
    return visited_count == len(nodes)

@app.post('/pipelines/parse')
def parse_pipeline(request: PipelineRequest):
    try:
        # Validate that all edge references point to existing nodes
        node_ids = {node.id for node in request.nodes}
        
        for edge in request.edges:
            if edge.source not in node_ids:
                raise HTTPException(
                    status_code=400,
                    detail=f"Edge references non-existent source node: {edge.source}"
                )
            if edge.target not in node_ids:
                raise HTTPException(
                    status_code=400,
                    detail=f"Edge references non-existent target node: {edge.target}"
                )
        
        # Check if graph is a DAG
        is_dag_result = is_dag(request.nodes, request.edges)
        
        return {
            'status': 'parsed',
            'nodes_count': len(request.nodes),
            'edges_count': len(request.edges),
            'is_dag': is_dag_result,
            'nodes': [node.dict() for node in request.nodes],
            'edges': [edge.dict() for edge in request.edges]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error parsing pipeline: {str(e)}"
        )
