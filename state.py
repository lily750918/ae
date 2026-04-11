"""
全局共享状态 — 供 ae_server.py、web/、loops.py 等模块引用。

用法：
    # ❌ 错误用法：from state import is_running (修改后其他模块不可见)
    # ✅ 正确用法：
    import state
    state.is_running = True
"""

strategy = None
is_running = False
start_time = None  # 系统启动时间（datetime, UTC）
