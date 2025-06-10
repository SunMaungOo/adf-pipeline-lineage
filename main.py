from config import (
    AZURE_SUBSCRIPTION_ID,
    AZURE_RESOURCE_GROUP_NAME,
    AZURE_DATA_FACTORY_NAME,
    OUTPUT_FILE_NAME
)
from client import DataFactoryClient
from typing import List,Optional,Set
from dataclasses import dataclass,asdict
from azure.mgmt.datafactory.models import PipelineResource
from graph import Edge,remove_node,get_first_nodes,get_disjointed_nodes,join_to_node,merge_edges
import json
from pathlib import Path

@dataclass
class Activity:
    name:str
    parents:List[str]
    is_execute_pipeline:bool
    execute_pipeline_name:Optional[str]

def get_activities(pipeline_resource:PipelineResource)->List[Activity]:

    activities:List[Activity] = list()

    for activity in pipeline_resource.activities:

        parents = [x.activity for x in activity.depends_on]

        is_execute_pipeline = (activity.type=="ExecutePipeline")

        execute_pipeline_name = None

        if is_execute_pipeline:
            execute_pipeline_name = activity.pipeline.reference_name

        activities.append(Activity(
            name=activity.name,\
            parents=parents,\
            is_execute_pipeline=is_execute_pipeline,\
            execute_pipeline_name=execute_pipeline_name
        ))

    return activities

def get_edge(activities:List[Activity])->List[Edge]:
    return [Edge(node_name=actv.name,\
                 parent_nodes=actv.parents) for actv in activities]

def join_as_parent(node_name:str,edges:List[Edge])->Optional[List[Edge]]:
    """
    node_name : node we wanted to join as parent 
    """
    first_node_edges = get_first_nodes(edges=edges)

    disjointed_node_edges = get_disjointed_nodes(edges=edges)

    unique_node_names:Set[str] = set()

    for x in first_node_edges:
        unique_node_names.add(x.node_name)

    for x in disjointed_node_edges:
        unique_node_names.add(x.node_name)

    new_edges = None


    for x in unique_node_names:
        new_edges = join_to_node(node_name=x,concate_edges=[
        Edge(
            node_name=node_name,
            parent_nodes=[]
        )
        ],edges=edges)

    return new_edges

def get_activity_lineage(activities:List[Activity])->List[Edge]:

    edges = get_edge(activities=activities)

    removable_node_name:List[str] = list()

    for x in activities:
        if not x.is_execute_pipeline:
            removable_node_name.append(x.name)

    #remove a non execute pipeline node

    for x in removable_node_name:
        edges = remove_node(node_name=x,edges=edges)

    return edges

def get_pipeline_lineage(pipelines:List[PipelineResource])->List[Edge]:

    lineage_edges:List[Edge] = list()

    for pipeline_resource in pipelines:
        activities = get_activities(pipeline_resource)

        pipeline_name = pipeline_resource.name

        lineage_edge = get_activity_lineage(activities=activities)

        # if there is the execute pipeline activity ,make the current pipeline parent

        if len(lineage_edge)>0:
            lineage_edge = join_as_parent(node_name=pipeline_name,\
                                          edges=get_activity_lineage(activities=activities)) 
        else:

            lineage_edge = [Edge(
                node_name=pipeline_name,
                parent_nodes=[]
            )]

        lineage_edges.append(lineage_edge)

    return merge_edges(graphs=lineage_edges)



def main():

    client = DataFactoryClient(subscription_id=AZURE_SUBSCRIPTION_ID,\
                               resource_group_name=AZURE_RESOURCE_GROUP_NAME,\
                               data_factory_name=AZURE_DATA_FACTORY_NAME)
    
    pipelines=client.get_all_pipelines()


    print("Generating pipeline lineage - started")

    lineage = get_pipeline_lineage(pipelines=pipelines)

    print("Generating pipeline lineage - completed")

    output_file_path = Path(f"data/{OUTPUT_FILE_NAME}")
    output_file_path.parent.mkdir(parents=True,exist_ok=True)

    print(f"Saving file {output_file_path.absolute().as_posix()} - started")

    with output_file_path.open("w") as file:
        json.dump([asdict(edge) for edge in lineage],file,indent=4)

    print(f"Saving file {output_file_path.absolute().as_posix()} - completed")

if __name__=="__main__":
    main()