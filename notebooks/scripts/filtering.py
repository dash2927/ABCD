import pandas as pd
import numpy as np

class SimilarityFiltering():
    def __init__(self, data, movie_thresh = 0.6, rho_thresh = 0.7,
                 random_state=1234):
        '''
        DESCRIPTION:
            Manages user-user filtering for netflix data
        INPUTS:
            data : pd.DataFrame : Dataframe for Netflix data
            movie_thresh : threshold proportion of random_ID's movies that a user must have rated to be retained
            rho_thresh : threshold a user's correlation coefficient must meet to be retained
        '''
        self.data = data
        self.movie_thresh = movie_thresh
        self.rho_thresh = rho_thresh
        self.random_state = random_state
        self.similar_users = None

    def save(self, filename):
        '''
        DESCRIPTION:
            Saves dataframe
        INPUTS:
            filename : string : the filename to save to
        '''
        self.similarity_df.to_csv(filename, header=True, index=False)

    def getTopn(self, n):
        '''
        DESCRIPTION:
            Gets the top n users based on correlation
        INPUT:
            n : int : Number of users
        OUTPUT:

        '''
        return self.similar_users.sort_values(by = 'corr', ascending=False).iloc[0:10,:]

    def getUserSimilarity(self, customerId, basedon="Movie_Id"):
        '''
        DESCRIPTION:
            Gets the similarity df using User-User Filtering
        INPUT:
            customerId : int : The selected customer ID to compare with other
            users
            n : int : top n number of users to consider
        '''
        np.random.seed(self.random_state)
        movies_watched = self.data[self.data["CustomerID"] == customerId][basedon].tolist()
        if len(movies_watched) == 0:
            print("ERROR: customerId not in data.")
            return 0
        movies_watched_df = self.data[self.data[basedon].isin(movies_watched)]
        m_count = self.movie_thresh*len(movies_watched)
        user_movie_count = movies_watched_df.groupby(["CustomerID"])[basedon].count()
        user_movie_count = user_movie_count.reset_index()
        user_movie_count.columns = ["CustomerID", "count"]
        users_same_movies = user_movie_count[user_movie_count["count"] > m_count].sort_values("count",
                                                                                              ascending=False)
        del(user_movie_count)
        similar_users = users_same_movies["CustomerID"].tolist()
        similar_users_df = self.data[self.data["CustomerID"].isin(similar_users)]
        movies_watched_df2 = movies_watched_df[movies_watched_df['CustomerID'].isin(similar_users)]
        movies_watched_df3 = movies_watched_df2.drop(['Date'], axis=1)
        movies_watched_df_pivot = movies_watched_df3.pivot(index=['CustomerID'], columns=[basedon], values="Rating")
        corr_df = movies_watched_df_pivot.transpose()
        corr_df2 = corr_df.corrwith(corr_df[customerId], method='pearson')
        corr_df3 = corr_df2.sort_values(ascending=False).drop_duplicates()
        corr_df4 = corr_df3.to_frame()
        corr_df4.rename(columns={0:'corr'}, inplace=True )
        corr_df5 = corr_df4.drop(axis=0, index = customerId)
        top_users = corr_df5[(corr_df5["corr"] >= self.rho_thresh)]
        del(corr_df5)
        top_users.reset_index(drop=False, inplace=True)
        similar_users_df2 = similar_users_df[similar_users_df['CustomerID'].isin(top_users['CustomerID'])]
        self.similar_users = pd.merge(similar_users_df2, top_users, how='inner')
        return 1
