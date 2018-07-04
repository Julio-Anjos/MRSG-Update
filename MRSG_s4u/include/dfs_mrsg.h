/* Copyright (c) 2012-2014. MRSG Team. All rights reserved. */

/* This file is part of MRSG.

MRSG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

MRSG is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with MRSG.  If not, see <http://www.gnu.org/licenses/>. */

#ifndef DFS_MRSG_HEADER
#define DFS_MRSG_HEADER

/** @brief  Matrix that maps chunks to workers. */
extern char**  chunk_owner_mrsg;

/**
 * @brief  Distribute chunks (and replicas) to DataNodes.
 */
void distribute_data_mrsg (void);

/**
 * @brief  Default data distribution algorithm.
 */
void default_mrsg_dfs_f (char** dfs_matrix, size_t chunks, size_t workers_mrsg, int replicas);

/**
 * @brief  Choose a random DataNode that owns a specific chunk.
 * @param  cid  The chunk ID.
 * @return The ID of the DataNode.
 */
size_t find_random_chunk_owner_mrsg (int cid);

/**
 * @brief  DataNode main function.
 *
 * Process that listens for data requests.
 */
//OLD int data_node_mrsg (int argc, char *argv[]);
void data_node_mrsg ();

#endif /* !DFS_MRSG_H */
