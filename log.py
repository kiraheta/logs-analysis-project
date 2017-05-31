#!/usr/bin/env python3

"""
This program is a reporting tool that prints out reports (in plain text)
based on the data in the database.

@author: Kenny Iraheta
@Date: 2017-05-23
"""

import psycopg2

DBNAME = "news"


def connect_to_db(DBNAME):
    """
    Connects to DBNAME
    """
    try:
        db = psycopg2.connect("dbname={}".format(DBNAME))
        cursor = db.cursor()
        return db, cursor
    except:
        print("Unable to connect to DB.")


def get_most_popular_articles():
    """
    Prints most popular articles from database DBNAME
    """
    db, c = connect_to_db(DBNAME)
    sql_query = """
                SELECT title,count(title)
                AS article_views
                FROM articles,log
                WHERE articles.slug = SUBSTRING(log.path,10,30)
                GROUP BY title
                ORDER BY article_views
                DESC LIMIT 3;
                """
    c.execute(sql_query)
    print("\nMost popular articles:")
    for article in c:
        print ('    ' + article[0] + ' - ' + str(article[1]) + ' views')
    db.close()


def get_most_popular_authors():
    """
    Prints most popular authors from database DBNAME
    """
    db, c = connect_to_db(DBNAME)
    sql_query = """
                SELECT authors.name
                AS author,
                count(articles.author)
                AS article_views
                FROM articles,authors,log
                WHERE articles.slug = SUBSTRING(log.path,10,30)
                AND articles.author = authors.id
                GROUP BY authors.name
                ORDER BY article_views
                DESC LIMIT 4;
                """
    c.execute(sql_query)
    print("\nMost popular authors:")
    for article in c:
        print ('    ' + article[0] + ' - ' + str(article[1]) + ' views')
    db.close()
    db.close()


def get_request_errors():
    """
    Prints days that have more than 1 percent of requests that lead to errors
    """
    db, c = connect_to_db(DBNAME)
    sql_query = """
                SELECT days,total_errors,total_requests,
                round((total_errors::numeric/total_requests::numeric)*100,2)
                AS percent_of_errors
                FROM (SELECT DATE_TRUNC('day', time)::date
                AS days, count(status)
                AS total_requests,
                SUM(
                    CASE WHEN status != '200 OK' THEN 1
                         ELSE 0
                    END) AS total_errors
                FROM log
                GROUP BY DATE_TRUNC('day', time)::date)
                AS end_result
                WHERE (total_errors::numeric/total_requests::numeric)*100 > 1.0
                ORDER BY percent_of_errors DESC;
                """
    c.execute(sql_query)
    print("\nDays with more than 1% errors:")
    for article in c:
        print('    ' + str(article[0]) + ' - ' + str(article[3]) + '% ' +
              'errors')
    db.close()

if __name__ == '__main__':
    get_most_popular_articles()
    get_most_popular_authors()
    get_request_errors()
