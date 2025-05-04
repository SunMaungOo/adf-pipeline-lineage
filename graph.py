from typing import List,Dict,Optional
from dataclasses import dataclass


@dataclass
class Edge:
    node_name:str
    parent_nodes:List[str]


def is_valid_edge(edge:Edge)->bool:
    return len(edge.parent_nodes)==len(set(edge.parent_nodes))

def is_valid_edges(edges:List[Edge])->bool:

    node_names:List[str] = list()

    for edge in edges:
        if not is_valid_edge(edge=edge):
            return False
        
        node_names.append(edge.node_name)

    return len(node_names)==len(set(node_names))

def remove_node(node_name:str,edges:List[Edge])->Optional[List[Edge]]:
    
    node = get_node(node_name=node_name,edges=edges)

    if node is None:
        return None

    if not is_node_parent(node_name=node_name,edges=edges):
        return force_remove_node(node_name=node_name,edges=edges)
    
    new_edges = force_remove_node(node_name=node_name,edges=edges)

    used_edges = get_used_edge(node_name=node_name,edges=new_edges)

    new_used_edges:List[Edge] = list()

    #prepare new edge 

    for edge in used_edges:

        used_parent_node = edge.parent_nodes

        #concate the parent node of the remove node to the node which is using that remove node

        for parent_node in node.parent_nodes:
            if parent_node not in used_parent_node:
                used_parent_node.append(parent_node)
        
        #remove the node that we are going to remove as a parents

        used_parent_node.remove(node.node_name)

        new_used_edges.append(Edge
                              (
                                  node_name=edge.node_name,
                                  parent_nodes=used_parent_node
                              ))

    new_parent_used_edge = edge_to_dict(new_used_edges)

    final_edge:List[Edge] = list()
    
    for edge in new_edges:

        # add the new edge that we have prepare

        if edge.node_name in new_parent_used_edge:

            final_edge.append(
                Edge
                (
                    node_name=edge.node_name,
                    parent_nodes=new_parent_used_edge[edge.node_name]
                )
            )

            continue

        final_edge.append(edge)
    
    return final_edge

def force_remove_node(node_name:str,edges:List[Edge])->List[Edge]:
    """
    Forcefully remove the node in edge
    Return edge with the node remove
    """

    new_edges:List[Edge] = list()

    for edge in edges:

        if edge.node_name==node_name:
            continue

        new_edges.append(edge)


    return new_edges


def is_node_parent(node_name:str,edges:List[Edge])->bool:
    """
    Return whether there is node which use the node as the parents
    """
    return len(get_used_edge(node_name=node_name,edges=edges))>0

def get_used_edge(node_name:str,edges:List[Edge])->List[Edge]:
    """
    Return which edge used the node name
    """
    
    used_edge:List[Edge] = list()

    for edge in edges:
        if node_name in edge.parent_nodes:
            used_edge.append(edge)

    return used_edge

def get_node(node_name:str,edges:List[Edge])->Optional[Edge]:
    
    for edge in edges:
        if node_name==edge.node_name:
            return edge
        
    return None

def edge_to_dict(edges:List[Edge])->Dict[str,List[str]]:
    return {x.node_name:x.parent_nodes for x in edges}

def merge_edge(left_edges:List[Edge],right_edges:List[Edge])->List[Edge]:
    
    merge_edge:List[Edge] = list()

    for left_node in left_edges:

        parents = left_node.parent_nodes

        for right_node in right_edges:
            """
            if there are same node in both edge
            concat the parent
            """            
            if left_node.node_name==right_node.node_name:
                for parent in right_node.parent_nodes:
                    if parent not in parents:
                        parents.append(parent)
        
        merge_edge.append(
            Edge
            (
                node_name=left_node.node_name,\
                parent_nodes=parents
            )
        )

    left_node_names = [node.node_name for node in left_edges]

    # node which only exists in right edges

    unqiue_right_nodes = [node for node in right_edges if not node.node_name in left_node_names]

    for node in unqiue_right_nodes:

        merge_edge.append(
            Edge
            (
                node_name=node.node_name,\
                parent_nodes=node.parent_nodes
            )
        )

    return merge_edge

def merge_edges(graphs:List[List[Edge]])->List[Edge]:

    if len(graphs)==1:
        return graphs[0]
    
    left_edge = graphs[0]

    merge_graph = None

    for index in range(1,len(graphs)):

        right_edges = graphs[index]

        merge_graph = merge_edge(left_edges=left_edge,\
                                 right_edges=right_edges)
        
        left_edge = merge_graph

    return merge_graph
    
def replace_nodes(node_name:str,replace_node_names:List[str],edges:List[Edge])->Optional[List[Edge]]:
    """
    Replace the node with new nodes in its places
    replace_node_names : new node which does not already exist
    """

    node = get_node(node_name=node_name,edges=edges)

    if node is None or node_name in replace_node_names:
        return None
    
    new_nodes:List[Edge] = list()

    for replace_node_name in replace_node_names:
        new_nodes.append(
            Edge
            (
                node_name=replace_node_name,\
                parent_nodes=node.parent_nodes
            )
        )

    edges.extend(new_nodes)

    used_edge = get_used_edge(node_name=node_name,edges=edges)

    if len(used_edge)>0:

        new_edge:List[Edge] = list()

        new_used_edge:List[Edge] = list()

        for node in used_edge:
            new_parents = node.parent_nodes

            #remove the node that we are going to replace as parent

            new_parents.remove(node_name)

            #add new node that we are going to replace with as parent

            for replace_node_name in replace_node_names:
                if replace_node_name not in new_parents:
                    new_parents.append(replace_node_name)

            new_used_edge.append(
                Edge
                (
                    node_name=node.node_name,\
                    parent_nodes=new_parents
                )
            )

        new_parent_used_edge = edge_to_dict(new_used_edge)
 
        for edge in edges:
            if edge.node_name in new_parent_used_edge:

                new_edge.append(
                    Edge
                    (
                        node_name=edge.node_name,
                        parent_nodes=new_parent_used_edge[edge.node_name]
                    )
                )

                continue

            new_edge.append(edge)

        edges = new_edge


    return remove_node(node_name=node_name,edges=edges)

def replace_node_parents(node_name:str,replace_node_names:List[str],edges:List[Edge])->Optional[List[Edge]]:
    """
    Just replace the node which is used as parent to the new nodes as parents
    replace_node_names : node which already exists
    """

    # the new node name cannot be same as org value

    if node_name in replace_node_names:
        return None
    
    # check if the node we wanted to replace actually exist

    if get_node(node_name=node_name,edges=edges) is None:
        return None
    
    # check if the node we are replacing the parent as actually exist

    for node in replace_node_names:
        if get_node(node_name=node,edges=edges) is None:
            return None

    used_edges = get_used_edge(node_name=node_name,edges=edges)

    used_edges = [x for x in used_edges if x.node_name not in replace_node_names]

    used_edges_dict = edge_to_dict(used_edges)

    new_used_edges:List[Edge] = list()

    for edge in used_edges:

        parent_nodes = edge.parent_nodes

        if edge.node_name in used_edges_dict:

            # remove the old parent node

            parent_nodes.remove(node_name)

            # add the new parent node

            for replace_node_name in replace_node_names:
                if edge.node_name!=replace_node_name and\
                    replace_node_name not in parent_nodes:

                    parent_nodes.append(replace_node_name)

        new_used_edges.append(edge)

    used_edges_dict = edge_to_dict(new_used_edges)

    new_edges:List[Edge] = list()

    for edge in edges:
        if edge.node_name in used_edges_dict:

            new_edges.append(
                Edge
                (
                    node_name=edge.node_name,
                    parent_nodes=used_edges_dict[edge.node_name]
                )
            )

            continue
        new_edges.append(edge)

    return new_edges

def replace_node_with_edge(node_name:str,replace_edges:List[Edge],edges:List[Edge])->Optional[List[Edge]]:
    """
    Replace the node with edges

    replace_edge : edge to replace with. The replace_edge should not be existing edge
    """

    if get_node(node_name=node_name,edges=edges) is None:
        return None

    first_nodes = get_first_nodes(edges=replace_edges)

    last_nodes = get_last_nodes(edges=replace_edges)

    disjointed_nodes = get_disjointed_nodes(edges=replace_edges)

    back_combine_edges = merge_edge(left_edges=last_nodes,\
                               right_edges=disjointed_nodes)

    back_combine_node_names = [x.node_name for x in back_combine_edges]

    new_edges = join_to_node(node_name=node_name,\
                 concate_edges=replace_edges,\
                 edges=edges)


    new_edges = replace_node_parents(node_name=node_name,\
                         replace_node_names=back_combine_node_names,\
                        edges=new_edges)
    
    front_combine_edges = merge_edge(left_edges=first_nodes,\
               right_edges=disjointed_nodes)
    
    front_combine_node_names = [x.node_name for x in front_combine_edges]

    remove_node_parents = edge_to_dict(new_edges)[node_name]


    #if the node we are going to remove have a parent

    if len(remove_node_parents)>0:

        reconnection_edges:List[Edge] = list()

        # we must have the put add parent of remove node to the front and disjointed node of replace edges

        for edge in new_edges:
            if edge.node_name in front_combine_node_names:
                parent_nodes = edge.parent_nodes

                for parent_node_name in remove_node_parents:
                    if parent_node_name not in parent_nodes:
                        parent_nodes.append(parent_node_name)

                reconnection_edges.append(
                    Edge
                    (
                        node_name=edge.node_name,
                        parent_nodes=parent_nodes
                    )
                )

                continue

            reconnection_edges.append(edge)

        new_edges = reconnection_edges

    new_edges = remove_node(node_name=node_name,\
                edges=new_edges)
    
    return new_edges

    
    

def get_disjointed_nodes(edges:List[Edge])->List[Edge]:
    """
    Get node which have no parent and is not used by other nodes
    """
    disjointed_nodes:List[Edge] = list()

    for edge in edges:
        if len(edge.parent_nodes)==0 and\
              not is_node_parent(node_name=edge.node_name,edges=edges):
            
            disjointed_nodes.append(edge)

    return disjointed_nodes    

def get_last_nodes(edges:List[Edge])->List[Edge]:
    """
    Get the last nodes (excluding disjointed nodes)
    """

    last_nodes:List[Edge] = list()

    for edge in edges:
        if len(edge.parent_nodes)>0 and\
        not is_node_parent(node_name=edge.node_name,edges=edges):
            
            last_nodes.append(edge)
        

    return last_nodes

def get_first_nodes(edges:List[Edge])->List[Edge]:
    """
    Get the first nodes (excluding disjointed nodes)
    """

    first_nodes:List[Edge] = list()

    for edge in edges:
        if len(edge.parent_nodes)==0 and\
              is_node_parent(node_name=edge.node_name,edges=edges):
            
            first_nodes.append(edge)


    return first_nodes


def join_to_node(node_name:str,concate_edges:List[Edge],edges:List[Edge])->Optional[List[Edge]]:
    """
    Join the concate_edge to the existing edges node
    concate_edges : new edge to join
    """

    if get_node(node_name=node_name,edges=edges) is None:
        return None

    first_nodes = get_first_nodes(edges=concate_edges)

    disjointed_nodes = get_disjointed_nodes(edges=concate_edges)

    # whether the join node already exist in new edges we wanted to concate

    has_join_nodes = get_node(node_name=node_name,edges=concate_edges) is not None

    new_concate_edges:List[Edge] = list()

    if has_join_nodes:

        new_disjointed_nodes:List[Edge] = list()

        for edge in disjointed_nodes:

            #if it is not a node that we wanted to join to add that join node as parents

            if edge.node_name!=node_name:

                new_disjointed_nodes.append(
                    Edge
                    (
                        node_name=edge.node_name,
                        parent_nodes=[node_name]
                    )
                )

                continue

            new_disjointed_nodes.append(edge)

        new_first_nodes:List[Edge] = list()

        for edge in first_nodes:

            #if it is not a node that we wanted to join , add that join node as parent nodes

            if edge.node_name!=node_name:
                parent_nodes = edge.parent_nodes
                parent_nodes.append(node_name)

                new_first_nodes.append(
                    Edge
                    (
                        node_name=edge.node_name,\
                        parent_nodes=parent_nodes
                    )
                )

                continue

            new_first_nodes.append(edge)

        for edge in concate_edges:

            first_node = get_node(edge.node_name,edges=new_first_nodes)

            is_in_first_node = first_node is not None

            disjointed_node = get_node(edge.node_name,edges=new_disjointed_nodes)

            is_in_disjointed_nodes = disjointed_node is not None

            if is_in_first_node:
                new_concate_edges.append(first_node)
            elif is_in_disjointed_nodes:
                new_concate_edges.append(disjointed_node)
            else:
                new_concate_edges.append(edge)

    else:

        # no join nodes has been found , we just have to add them as new edge

        for edge in concate_edges:

            is_in_first_node = get_node(edge.node_name,edges=first_nodes) is not None

            is_in_disjointed_nodes = get_node(edge.node_name,edges=disjointed_nodes) is not None

            #add the node that we wanted to join as parent 

            if is_in_first_node or is_in_disjointed_nodes:

                parent_nodes = edge.parent_nodes
                parent_nodes.append(node_name)

                new_concate_edges.append(
                    Edge
                    (
                        node_name=edge.node_name,\
                        parent_nodes=parent_nodes
                    )
                )

                continue

            new_concate_edges.append(edge)
            
    return merge_edge(left_edges=edges,right_edges=new_concate_edges)