using System;
using System.Collections.Generic;
using System.IO;
using Voron.Impl;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Diagnostics;
using Voron.Util.Conversion;
using System.Runtime.InteropServices;
using Newtonsoft.Json.Linq;

namespace Voron.Graph
{
    public class Session : ISession
    {
        private readonly GraphEnvironment _graphEnvironment;
        private WriteBatch _writeBatch;
        private SnapshotReader _snapshot;

        internal Session(GraphEnvironment graphEnvironment)
        {
            _graphEnvironment = graphEnvironment;
            _snapshot = _graphEnvironment.StorageEnvironment.CreateSnapshot();
            _writeBatch = new WriteBatch();
        }

        public Iterator<Node> IterateNodes()
        {
            var iterator = _snapshot.Iterate(_graphEnvironment.NodeTreeName, _writeBatch);

            return new Iterator<Node>(iterator,
                (key, value) =>
                {
                    using (value)
                    {
                        value.Position = 0;
                        var node = new Node(key.CreateReader().ReadBigEndianInt64(), value.ToJObject());

                        return node;
                    }
                });
        }

        public Iterator<Edge> IterateEdges()
        {
            var iterator = _snapshot.Iterate(_graphEnvironment.EdgeTreeName, _writeBatch);

            return new Iterator<Edge>(iterator,
                (key, value) =>
                {
                    using (value)
                    {
                        var currentKey = key.ToEdgeTreeKey();
                        var jsonValue = value.Length > 0 ? value.ToJObject() : new JObject();

                        var edge = new Edge(currentKey.NodeKeyFrom, currentKey.NodeKeyTo, jsonValue, currentKey.Type);

                        return edge;
                    }
                });
        }

        public void SaveChanges()
        {
            _graphEnvironment.StorageEnvironment.Writer.Write(_writeBatch);
            _writeBatch.Dispose();
            _writeBatch = new WriteBatch();
        }

        public Node CreateNode(dynamic value)
        {
            return CreateNode(Util.ConvertToJObject(value));
        }


        public Node CreateNode(JObject value)
        {
            if (value == null) throw new ArgumentNullException("value");

            var key = _graphEnvironment.Conventions.GenerateNextNodeIdentifier();

            var nodeKey = key.ToSlice();

            _writeBatch.Add(nodeKey, value.ToStream(), _graphEnvironment.NodeTreeName);
            _writeBatch.Add(nodeKey, Stream.Null, _graphEnvironment.DisconnectedNodesTreeName);

            return new Node(key, value);
        }

        public Edge CreateEdgeBetween(Node nodeFrom, Node nodeTo, dynamic value, ushort type = 0)
        {
            return CreateEdgeBetween(nodeFrom, nodeTo, Util.ConvertToJObject(value), type);
        }

        public Edge CreateEdgeBetween(Node nodeFrom, Node nodeTo, JObject value = null, ushort type = 0)
        {
            if (nodeFrom == null) throw new ArgumentNullException("nodeFrom");
            if (nodeTo == null) throw new ArgumentNullException("nodeTo");

            var edge = new Edge(nodeFrom.Key, nodeTo.Key, value);
            _writeBatch.Add(edge.Key.ToSlice(), value.ToStream() ?? Stream.Null, _graphEnvironment.EdgeTreeName);

            _writeBatch.Delete(nodeFrom.Key.ToSlice(), _graphEnvironment.DisconnectedNodesTreeName);

            return edge;
        }

        public void Delete(Node node)
        {
            var nodeKey = node.Key.ToSlice();
            _writeBatch.Delete(nodeKey, _graphEnvironment.NodeTreeName);
            _writeBatch.Delete(nodeKey, _graphEnvironment.DisconnectedNodesTreeName); //just in case, doesn't have to be here

            //TODO: Where are we deleting the edges?
        }

        public void Delete(Edge edge)
        {
            var edgeKey = edge.Key.ToSlice();
            _writeBatch.Delete(edgeKey, _graphEnvironment.EdgeTreeName);
        }

        public IEnumerable<Node> GetAdjacentOf(Node node, ushort type)
        {
            var alreadyRetrievedKeys = new HashSet<long>();
            using (var edgeIterator = _snapshot.Iterate(_graphEnvironment.EdgeTreeName, _writeBatch))
            {
                var sliceWriter = new SliceWriter(sizeof(long) + sizeof(ushort));
                sliceWriter.WriteBigEndian(node.Key);
                sliceWriter.WriteBigEndian(type);
                edgeIterator.RequiredPrefix = sliceWriter.CreateSlice();
                    
                if (!edgeIterator.Seek(Slice.BeforeAllKeys))
                    yield break;

                do
                {
                    var edgeKey = edgeIterator.CurrentKey.ToEdgeTreeKey();

                    if (!alreadyRetrievedKeys.Contains(edgeKey.NodeKeyTo))
                    {
                        alreadyRetrievedKeys.Add(edgeKey.NodeKeyTo);
                        var adjacentNode = LoadNode(edgeKey.NodeKeyTo);
                        yield return adjacentNode;
                    }

                } while (edgeIterator.MoveNext());
            }
        }

        public bool IsIsolated(Node node)
        {
            using (var edgeIterator = _snapshot.Iterate(_graphEnvironment.EdgeTreeName, _writeBatch))
            {
                edgeIterator.RequiredPrefix = node.Key.ToSlice();
                return edgeIterator.Seek(Slice.BeforeAllKeys);
            }
        }


        public Node LoadNode(long nodeKey)
        {
            var readResult = _snapshot.Read(_graphEnvironment.NodeTreeName, nodeKey.ToSlice(), _writeBatch);
            if (readResult == null)
                return null;

            using (var valueStream = readResult.Reader.AsStream())
                return new Node(nodeKey, valueStream.ToJObject());
        }


        public IEnumerable<Edge> GetEdgesBetween(Node nodeFrom, Node nodeTo, Func<ushort, bool> typePredicate = null)
        {
            if (nodeFrom == null)
                throw new ArgumentNullException("nodeFrom");
            if (nodeTo == null)
                throw new ArgumentNullException("nodeTo");

            using (var edgeIterator = _snapshot.Iterate(_graphEnvironment.EdgeTreeName, _writeBatch))
            {
                edgeIterator.RequiredPrefix = Util.EdgeKeyPrefix(nodeFrom, nodeTo);
                if (!edgeIterator.Seek(edgeIterator.RequiredPrefix))
                    yield break;

                do
                {
                    var edgeTreeKey = edgeIterator.CurrentKey.ToEdgeTreeKey();
                    if (typePredicate != null && !typePredicate(edgeTreeKey.Type))
                        continue;

                    var valueReader = edgeIterator.CreateReaderForCurrent();
                    using (var valueStream = valueReader.AsStream() ?? Stream.Null)
                    {
                        var jsonValue = valueStream.Length > 0 ? valueStream.ToJObject() : new JObject();
                        yield return new Edge(edgeTreeKey, valueStream.ToJObject());
                    }

                } while (edgeIterator.MoveNext());
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool isDisposing)
        {
            if (_snapshot != null)
            {
                _snapshot.Dispose();
                _snapshot = null;
            }

            if (isDisposing)
                GC.SuppressFinalize(this);
        }

        ~Session()
        {
#if DEBUG
            if (_snapshot != null)
                Trace.WriteLine("Disposal for Session object was not called, disposing from finalizer. Stack Trace: " + new StackTrace());
#endif
            Dispose(false);
        }
    }
}