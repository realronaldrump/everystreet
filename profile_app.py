
import cProfile
import pstats
import io
from app import app  # Import your main Quart app

def profile_app():
    pr = cProfile.Profile()
    pr.enable()

    # Run your app for a short period
    app.run(debug=False, port=5000)

    pr.disable()
    s = io.StringIO()
    sortby = 'cumulative'
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())

if __name__ == "__main__":
    profile_app()
