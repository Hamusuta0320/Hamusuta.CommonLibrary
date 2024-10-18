using System.Threading.Tasks.Dataflow;

namespace Hamusuta.CommonLibrary.Concurrent
{
    public class ConcurrentTaskExecutor
    {
        private readonly int _maxConcurrent;
        private readonly IEnumerable<Func<Task>> _taskList;

        public ConcurrentTaskExecutor(int maxConcurrent, IEnumerable<Func<Task>> taskList)
        {
            _maxConcurrent = maxConcurrent;
            _taskList = taskList;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ensureOrdered"></param>
        /// <returns></returns>
        public async Task Execute(bool ensureOrdered = false)
        {
            var p = new ActionBlock<Func<Task>>(async t => await t.Invoke(), new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = _maxConcurrent,
                EnsureOrdered = ensureOrdered
            });
            foreach (var t in _taskList)
            {
                await p.SendAsync(t);
            }
            p.Complete();
            await p.Completion;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task ExecuteWhenAny()
        {
            var cancelToken = new CancellationTokenSource();
            var hasAny = new TaskCompletionSource();
            var p = new ActionBlock<Func<Task>>(async t =>
            {
                if (cancelToken.IsCancellationRequested) return;
                await t.Invoke();
                hasAny.SetResult();
                cancelToken.Cancel();
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = _maxConcurrent
            });
            foreach (var t in _taskList)
            {
                await p.SendAsync(t, cancelToken.Token);
            }
            _ = p.Completion.ContinueWith(_ => hasAny.SetResult(), cancelToken.Token);
            p.Complete();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="maxConcurrent"></param>
        /// <param name="taskList"></param>
        /// <returns></returns>
        public static ConcurrentTaskExecutor Create(int maxConcurrent, IEnumerable<Func<Task>> taskList) => new(maxConcurrent, taskList);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="maxConcurrent"></param>
        /// <param name="source"></param>
        /// <param name="taskCreator"></param>
        /// <returns></returns>
        public static ConcurrentTaskExecutor Create<T>(int maxConcurrent, IEnumerable<T> source, Func<T, Func<Task>> taskCreator)
        {
            var tasks = new List<Func<Task>>();
            foreach (var t in source)
            {
                tasks.Add(taskCreator.Invoke(t));
            }
            return new(maxConcurrent, tasks);
        }
    }

    public class ConcurrentTaskExecutor<TResult>
    {
        private readonly int _maxConcurrent;
        private readonly IEnumerable<Func<Task<TResult>>> _taskList;

        public ConcurrentTaskExecutor(int maxConcurrent, IEnumerable<Func<Task<TResult>>> taskList)
        {
            _maxConcurrent = maxConcurrent;
            _taskList = taskList;
        }

        public async Task<List<TResult>> Execute()
        {
            var p = new TransformBlock<Func<Task<TResult>>, TResult>(async t => await t.Invoke(), new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = _maxConcurrent,
                EnsureOrdered = true
            });

            var result = new List<TResult>();
            var saveBlock = new ActionBlock<TResult>(r => result.Add(r));

            p.LinkTo(saveBlock, new DataflowLinkOptions { PropagateCompletion = true });

            foreach (var t in _taskList)
            {
                await p.SendAsync(t);
            }
            p.Complete();
            await saveBlock.Completion;
            return result;
        }

        public async Task<TResult> ExecuteWhenAny()
        {
            var cancelToken = new CancellationTokenSource();
            var hasAny = new TaskCompletionSource<TResult>();
            var p = new ActionBlock<Func<Task<TResult>>>(async t =>
            {
                if (cancelToken.IsCancellationRequested) return;
                var r = await t.Invoke();
                hasAny.SetResult(r);
                cancelToken.Cancel();
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = _maxConcurrent
            });
            foreach (var t in _taskList)
            {
                await p.SendAsync(t, cancelToken.Token);
            }
            _ = p.Completion.ContinueWith(_ => hasAny.SetResult(default), cancelToken.Token);
            p.Complete();
            return await hasAny.Task;
        }

        public async Task<TResult> ExecuteWhenAny(Predicate<TResult> predicate, TResult defaultValue)
        {
            var cancelToken = new CancellationTokenSource();
            var hasAny = new TaskCompletionSource<TResult>();
            var p = new ActionBlock<Func<Task<TResult>>>(async t =>
            {
                if (cancelToken.IsCancellationRequested) return;
                var r = await t.Invoke();
                if (predicate != null && !predicate.Invoke(r)) return;
                hasAny.SetResult(r);
                cancelToken.Cancel();
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = _maxConcurrent
            });
            foreach (var t in _taskList)
            {
                await p.SendAsync(t, cancelToken.Token);
            }
            _ = p.Completion.ContinueWith(_ => hasAny.SetResult(defaultValue), cancelToken.Token);
            p.Complete();
            return await hasAny.Task;
        }

        public static ConcurrentTaskExecutor<TResult> Create(int maxConcurrent, IEnumerable<Func<Task<TResult>>> taskList) => new(maxConcurrent, taskList);

        public static ConcurrentTaskExecutor<TResult> Create<T>(int maxConcurrent, IEnumerable<T> source, Func<T, Func<Task<TResult>>> taskGenerator)
        {
            var tasks = new List<Func<Task<TResult>>>();

            foreach (var t in source)
            {
                tasks.Add(taskGenerator.Invoke(t));
            }

            return new(maxConcurrent, tasks);
        }
    }
}
