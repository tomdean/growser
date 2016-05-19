var TableList = React.createClass({
    render: function() {
        var list = this.props.results.map(function(rec) {
            return <TableListItem repo={rec.repo} key={rec.repo.repo_id} />;
        });
        return (
            <table className="table table-striped">
                <thead>
                <tr>
                    <th>Name</th>
                    <th>Language</th>
                    <th>Users</th>
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

var TableListItem = React.createClass({
    render: function() {
        var repo= this.props.repo;
        var lang_url = "/b/" + repo.language;
        var num_events = parseInt(repo.num_events).toLocaleString()
        var github_url = "https://github.com/" + repo.name
        return (
            <tr>
                <td style={{minWidth: 350}}>
                    <a href={ github_url } className="gh_icon" target="_blank"><img src="/static/github.png" /></a>
                    <a href={repo.url}>{repo.name}</a>
                </td>
                <td style={{whiteSpace:'nowrap'}}><a href={lang_url}>{repo.language}</a></td>
                <td className="small">{num_events}</td>
                <td className="small">{repo.description}</td>
            </tr>
        );
    }
});

var GridList = React.createClass({
    getInitialState: function() {
        return {data: {"results": [1, 2, 3]}};
    },
    render: function() {
        var list = this.props.results.map(function(rec) {
            return <GridListItem key={rec.repo.repo_id} repo={rec.repo}/>;
        });
        return (<div id="repositories">{list}</div>);
    }
});

var GridListItem = React.createClass({
    componentDidMount: function() {
        $(function () {
          $('[data-toggle="tooltip"]').tooltip()
        })
    },
    render: function() {
        return(
            <div className="repository">
                <div className="title">
                    <a href={this.props.repo.url}>{this.props.repo.name}</a><br/>
                    {this.props.repo.last_release_at}
                </div>
                <a href={this.props.repo.url} className="repository-image" style={{backgroundImage: 'url(' + this.props.repo.image + ')' }} title={ this.props.repo.description } data-toggle="tooltip" data-placement="bottom"></a>
            </div>
        );
    }
});

var Repositories = React.createClass({
    getInitialState: function() {
        return {"component": false, "results": []};
    },
    componentWillMount: function() {
        $.getJSON(this.props.url, function(data) {this.setState(data)}.bind(this));
    }, toggle: function() {
        this.setState({component: !this.state.component});
    },
    render: function() {
        var results = (this.state.component) ? <TableList results={this.state.results} />
                                             : <GridList results={this.state.results} />;
        return(
            <div id="recommendations">
                <div className="pull-right">
                    View as &nbsp;
                    <span className="glyphicon glyphicon-th-list" title="List" onClick={this.toggle}></span>&nbsp;
                    <span className="glyphicon glyphicon-th" title="Images" onClick={this.toggle}></span>
                </div>
                <h4>Recommendations</h4>
                {results}
            </div>
        );
    }
});

ReactDOM.render(
    <Repositories url={"/api/v1/recommendations/" + model_id + "/" + repo_id} />,
    document.getElementById('repository-list')
);
