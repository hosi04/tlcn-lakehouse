from __future__ import annotations
from typing import Any, Optional
from typing_extensions import TypedDict


class AgentState(TypedDict, total=False):
    question: str                       
    intent: str                         
    direct_answer: Optional[str]        

    retrieved_tables: list[str]         
    full_schemas: dict[str, Any]        

    pruned_schema: str                  
    columns_pruned_count: int           

    sql: str                            
    sql_error: Optional[str]           
    retry_count: int                    

    query_result: list[dict[str, Any]]  
    columns: list[str]                  
    row_count: int                      

    chart_config: dict[str, Any]        
    report: str                         
    schemas_used: list[str]             
    execution_log: list[str]            
