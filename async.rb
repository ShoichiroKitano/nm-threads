def get_state
  state = Thread.current.thread_variable_get(:state)
  Thread.current.thread_variable_set(:state, nil)
  state || State.new
end

def push_micro_task(state, &task)
  queue = Thread.current.thread_variable_get(:microtask_queue)
  queue << MicroTask.new(state, task)
end

def pop_micro_task
  Thread.current.thread_variable_get(:microtask_queue).shift
end

class Promise
  attr_reader :state, :result

  def initialize
    @state = :pending
    @result = nil
  end

  def set_result(result)
    @result = result
    @state = :done
  end
end

class State
  attr_accessor :step, :promise

  def initialize(step = 1)
    @step = step
    @variables = {}
    @promises = {}
    @promise = Promise.new
  end

  def set_variable(name, value)
    @variables[name] = value
  end

  def get_variable(name)
    @variables[name]
  end

  def set_promise(name, promise)
    @promises[name] = promise
  end

  def get_promise(name)
    @promises[name]
  end
end

class MicroTask
  def initialize(state, task)
    @state = state
    @task = task
  end

  def execute
    Thread.current.thread_variable_set(:state, @state)
    @task.call
  ensure
    Thread.current.thread_variable_set(:state, nil)
  end
end

def async_x
  # promise = async_y
  # y = await promise
  # promise = async_z
  # z = await promise
  # return y + z + 2
  state = get_state

  case state.step
  when 1
    state.set_promise(:promise_y, async_y)
    state.step = 2
    push_micro_task(state) { async_x }
  when 2
    promise_y = state.get_promise(:promise_y)
    unless promise_y.state == :done
      push_micro_task(state) { async_x }
      return state.promise
    end

    state.set_variable(:y, promise_y.result)
    state.set_promise(:promise_z, async_z)
    state.step = 3
    push_micro_task(state) { async_x }
  when 3
    promise_z = state.get_promise(:promise_z)
    unless promise_z.state == :done
      push_micro_task(state) { async_x }
      return state.promise
    end

    state.set_variable(:z, promise_z.result)
    y = state.get_variable(:y)
    z = state.get_variable(:z)
    state.promise.set_result(y + z + 2)
  end

  state.promise
end

# 一番レイヤーの低い非同期関数
def async_y
  # return 1
  state = get_state

  case state.step
  when 1
    state.step = 2
    push_micro_task(state) { async_y }
  when 2
    state.promise.set_result(1)
  end

  state.promise
end

# 一番レイヤーの低い非同期関数
def async_z
  # return 2
  state = get_state

  case state.step
  when 1
    state.step = 2
    push_micro_task(state) do
      push_micro_task(state) { async_z }
    end
  when 2
    state.promise.set_result(2)
  end

  state.promise
end

def event_loop(&async_function)
  Thread.current.thread_variable_set(:microtask_queue, [])
  Thread.current.thread_variable_set(:state, nil)
  state = State.new

  Thread.current.thread_variable_set(:state, state)
  promise = async_function.call
  Thread.current.thread_variable_set(:state, nil)

  until promise.state == :done
    task = pop_micro_task
    raise 'event loop has no tasks, but the promise is still pending' unless task

    task.execute

    # Resume the async block after each microtask, using its own saved state.
    Thread.current.thread_variable_set(:state, state)
    resumed_promise = async_function.call
    Thread.current.thread_variable_set(:state, nil)
    unless resumed_promise.equal?(promise)
      raise 'event-loop block returned a different Promise after resuming'
    end
  end

  promise.result
ensure
  Thread.current.thread_variable_set(:state, nil)
  Thread.current.thread_variable_set(:microtask_queue, nil)
end

def main
  result = event_loop do
    # promise = async_x
    # x = await promise
    # return x + 10
    state = get_state

    case state.step
    when 1
      state.set_promise(:promise_x, async_x)
      state.step = 2
    when 2
      promise_x = state.get_promise(:promise_x)
      # 本当はここで自分自身をマイクロタスクにして戻したい。
      state.promise.set_result(promise_x.result + 10) if promise_x.state == :done
    end

    state.promise
  end

  puts result
end

main
