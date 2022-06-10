#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
import matplotlib.pyplot as plt
import networkx as nx

class GraphDataset(object):
    def __init__(self, \
                 topo_df=None, \
                 relationship_to_flow=None, \
                 simulated_nodes=None) -> None:
        """
        Base class for generation a graph object.
        Initialize the GraphDataset object with topology dataframe, relationship selected for topology flow, and the list of source nodes to simulate time-series.

        Parameters
        ----------
        topo_df : topology table containing relationships among nodes, with columns=['relationshipName', 'sourceId', 'targetId'],
            pd.DataFrame (e.g.  index   relationshipName  sourceId  targetId
                                  0	        isParent	     A	       C
                                  1	        isParent	     A	       D
                                  2	        isParent	     A         E
                                  3	        isParent	     B	       E
                                  4	       isRedundant	     I	       F
                                  5	       isRedundant	     F	       I )
        relationship_to_flow : Choose which relationship to use for topology top-down flow,
            str (e.g. 'isParent')
        simulated_nodes : List of nodes selected as sources to simulate time-series,
            list of str (e.g. ['A', 'B'])
            
        Return
        ----------
        None
        """
        self.topo_df = topo_df
        self.relationship_to_flow = relationship_to_flow
        self.simulated_nodes = simulated_nodes
        self.G = self.get_graph()

    def get_graph(self) -> nx.Graph:
        """
        Obtain a networkx.Graph() object based on attributes specified in class initiation.

        Return
        ----------
        G : networkx Graph object
        """
        # Create a directed graph and sort the order of nodes
        G = nx.DiGraph()
        G.add_nodes_from(sorted(list(set(list(self.topo_df['sourceId'])+list(self.topo_df['targetId'])))))

        # Add edges of the graph and set relationship attribute and color for each edge
        edge_color_map = {'isParent': 'black', 'isRedundant': 'orange'}
        topo_df_gb = self.topo_df.groupby('relationshipName')
        for gb_key in topo_df_gb.groups.keys():
            sub_topo_df = topo_df_gb.get_group(gb_key)
            G.add_edges_from(list(zip(sub_topo_df['sourceId'], sub_topo_df['targetId'])), \
                             color=edge_color_map[gb_key], 
                             relationship=gb_key)
        # Save the graph for the object
        return G

    def plot_graph(self, \
                   sln_mat=None) -> None:
        """
        Plot the topology graph with or without a snapshot of telemetry for each node.

        Parameters
        ----------
        sln_mat : Solution matrix obtained from function populate_flow_all_nodes() to be included on the plot as node labels, 
            np.array, optional

        Return
        ----------
        None
        """
        sub_G = nx.DiGraph(((source, target, attr) for source, target, attr in (self.G).edges(data=True) \
                            if attr['relationship']==self.relationship_to_flow))
        adj_mat = nx.to_numpy_array(sub_G)
        div_factor = np.sum(adj_mat,axis=1)
        div_factor[div_factor==0] = 1
        edge_weight_mat = (adj_mat/div_factor[:,None]).round(2)
        
        G_tmp = nx.from_numpy_matrix(edge_weight_mat, create_using=nx.DiGraph)
        node_labels_map = dict(zip(G_tmp.nodes, sub_G.nodes))
        G_tmp = nx.relabel_nodes(G_tmp, node_labels_map)
        edge_labels = nx.get_edge_attributes(G_tmp, "weight")
        del G_tmp
        edge_color = [(self.G)[u][v]['color'] for u,v in (self.G).edges]
        
        if sln_mat is not None:
            node_labels_plot = [list((self.G).nodes)[i]+'\n'+str(sln_mat[i]) for i in range(len(sln_mat))]
            node_labels_plot_map = dict(zip((self.G).nodes, node_labels_plot))
        node_color_lst = ['green' if node in self.simulated_nodes else '#1f78b4' for node in (self.G).nodes]

        plt.figure(figsize=(10, 5))
        layout = nx.spring_layout(self.G)
        nx.draw(self.G, layout, edge_color=edge_color, arrowsize=60, width=3)
        nx.draw_networkx_nodes(self.G, layout, node_color=node_color_lst, node_size=1500)
        nx.draw_networkx_edge_labels(self.G, layout, edge_labels=edge_labels)
        if sln_mat is not None:
            nx.draw_networkx_labels(self.G, layout, font_size=10, labels=node_labels_plot_map, font_weight='normal')
        else:
            nx.draw_networkx_labels(self.G, layout, font_size=10, font_weight='normal')
        plt.show(block=False)
