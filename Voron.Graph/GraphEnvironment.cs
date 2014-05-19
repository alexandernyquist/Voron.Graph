using System;
using System.Threading;

namespace Voron.Graph
{
    public class GraphEnvironment
    {
        private readonly StorageEnvironment _storageEnvironment;
        private readonly string _nodeTreeName;
        private readonly string _edgeTreeName;
        private readonly string _disconnectedNodesTreeName;


        public GraphEnvironment(string graphName, StorageEnvironment storageEnvironment)
        {
            if (String.IsNullOrWhiteSpace(graphName)) throw new ArgumentNullException("graphName");
            if (storageEnvironment == null) throw new ArgumentNullException("storageEnvironment");
            _nodeTreeName = graphName + Constants.NodeTreeNameSuffix;
            _edgeTreeName = graphName + Constants.EdgeTreeNameSuffix;
            _disconnectedNodesTreeName = graphName + Constants.DisconnectedNodesTreeName;
            _storageEnvironment = storageEnvironment;

            long next = 0;
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = _storageEnvironment.CreateTree(tx, _nodeTreeName);
                _storageEnvironment.CreateTree(tx, _edgeTreeName);
                _storageEnvironment.CreateTree(tx, _disconnectedNodesTreeName);
                using (var it = tree.Iterate(tx))
                {
                    if (it.Seek(Slice.AfterAllKeys))
                    {
                        next = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                    }
                }
            }

            Conventions = new Conventions
            {
                GenerateNextNodeIdentifier = () => Interlocked.Increment(ref next)
            };
        }

        public Conventions Conventions { get; private set; }

        public ISession OpenSession()
        {
            return new Session(this);
        }

        public StorageEnvironment StorageEnvironment
        {
            get { return _storageEnvironment; }
        }

        public string NodeTreeName
        {
            get { return _nodeTreeName; }
        }

        public string EdgeTreeName
        {
            get { return _edgeTreeName; }
        }

        public string DisconnectedNodesTreeName
        {
            get { return _disconnectedNodesTreeName; }
        }
    }
}
