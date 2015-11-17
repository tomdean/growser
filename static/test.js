var RepositoryItem = React.createClass({
    render: function() {
        var rec = this.props.rec;
        var repo_url = "/r/" + rec.repo.name;
        var lang_url = "/l/" + rec.repo.language;
        var github_url = "https://github.com/" + rec.repo.name;
        return (
            <tr className="small">
                <td>
                    <a href={github_url} className="octicon octicon-mark-github" target="_blank"></a> &nbsp;
                    <a href={repo_url}>{rec.repo.name}</a>
                </td>
                <td><a href={lang_url}>{rec.repo.language}</a></td>
                <td>{rec.score.toFixed(6)}</td>
                <td>{rec.repo.description}</td>
            </tr>
        );
    }
});

var RepositoryTable = React.createClass({
    getInitialState: function() {
        return {data: {"recommendations": []}};
    },
    render: function() {
        var list = this.props.recommendations.map(function(rec) {
            return (
                <RepositoryItem rec={rec} key={rec.repo.repo_id} />
            );
        });
        return (
            <table className="table">
                <thead>
                <tr>
                    <th>Name</th>
                    <th>Language</th>
                    <th>Score</th>
                    <th>Description</th>
                </tr>
                </thead>
                <tbody>
                {list}
                </tbody>
            </table>
        );
    }
});

var RepositoryList = React.createClass({
    getInitialState: function() {
        return {data: {"results": [], "model": {"name": ""}}};
    },
    componentDidMount: function() {
        $.ajax({
            url: this.props.url,
            dataType: 'json',
            cache: false,
            success: function(data) {
                this.setState({data: data});
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    render: function() {
        return (
            <div>
                <h4>{ this.state.data.model.name }</h4>
                <RepositoryTable recommendations={this.state.data.results} />
            </div>
        );
    }
});

var url = "/api/v1/recommendations/1/" + repo_id;
ReactDOM.render(
    <RepositoryList url={url} />,
    document.getElementById('repository-list')
);
