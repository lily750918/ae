"""
全局共享状态 — 供 ae_server.py、web/、loops.py 等模块引用。

用法：
    from state import strategy, is_running, start_time
    # 修改时直接赋值：
    import state
    state.is_running = True
"""

strategy = None
is_running = False
start_time = None  # 系统启动时间（datetime, UTC）
